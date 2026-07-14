"""Run the ontology-package runtime query pipeline.

The runtime pipeline starts after onboarding has produced an ontology package.
This module is the runtime orchestrator: it reads package metadata/settings,
retrieves relevant RAG chunks, renders the initial generation prompt, runs the
candidate-query attempt loop, persists the query trace, and returns the response
shape used by the CLI and API.

The attempt loop also lives here on purpose. Each iteration validates the
candidate SPARQL, executes it when validation passes, records validation and
execution outcomes, and asks the LLM for a corrected candidate when needed.
Helper modules perform individual actions only: query generation, correction
generation, endpoint execution, prompt rendering, and formal validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from app.core.config import settings
from app.domain.package import (
    metadata_path,
    ontology_context_path,
    query_log_path,
    query_readable_latest_path,
    query_readable_runs_dir,
    read_json_file,
    resolve_package_dir,
    settings_path,
)
from app.domain.rag import RetrievedABoxChunk, RetrievedChunk, retrieve_abox_context, retrieve_context
from app.domain.runtime import query_correction, query_generation, sparql_execution
from app.domain.runtime.prompt_renderer import SYSTEM_ROLE, render_query_generation_prompt
from app.domain.runtime.query_trace import write_query_trace, write_readable_query_trace
from app.domain.runtime.validation import ValidationStageResult, validate_query
from app.domain.rag.few_shot_retrieval import retrieve_few_shot_examples
from app.domain.runtime.abox_path_discovery import discover_paths_for_correction

@dataclass(frozen=True)
class QueryPipelineResult:
    """Response returned by the public runtime pipeline.

    This result is intentionally close to the API response and CLI output. It
    includes the package/query metadata, retrieved context that influenced the
    prompt, generated and corrected SPARQL states, the endpoint execution result,
    final status, errors, and the path to the persisted trace.
    """

    question: str
    dataset_name: str
    dataset_endpoint: str
    model_name: str
    retrieved_context: list[dict[str, object]]
    retrieved_abox_context: list[dict[str, object]]
    chunking_strategy: str
    retrieval_top_k: int
    abox_retrieval_top_k: int
    use_abox_rag: bool
    use_reactive_abox_discovery: bool
    correction_max_iterations: int
    generated_sparql: str | None
    validated_sparql: str | None
    corrected_sparql: str | None
    execution_result: dict[str, object] | None
    status: str
    errors: list[str] | None
    trace_path: str
    readable_trace_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "question": self.question,
            "dataset_name": self.dataset_name,
            "dataset_endpoint": self.dataset_endpoint,
            "model_name": self.model_name,
            "retrieved_context": self.retrieved_context,
            "retrieved_abox_context": self.retrieved_abox_context,
            "chunking_strategy": self.chunking_strategy,
            "retrieval_top_k": self.retrieval_top_k,
            "abox_retrieval_top_k": self.abox_retrieval_top_k,
            "use_abox_rag": self.use_abox_rag,
            "use_reactive_abox_discovery": self.use_reactive_abox_discovery,
            "correction_max_iterations": self.correction_max_iterations,
            "generated_sparql": self.generated_sparql,
            "validated_sparql": self.validated_sparql,
            "corrected_sparql": self.corrected_sparql,
            "execution_result": self.execution_result,
            "status": self.status,
            "errors": self.errors,
            "trace_path": self.trace_path,
            "readable_trace_path": self.readable_trace_path,
        }


async def run_query_pipeline(
    question: str,
    package_dir: str | Path,
    *,
    model: str | None = None,
    endpoint: str | None = None,
    k: int | None = None,
    chunking: str | None = None,
    use_abox_rag: bool | None = None,
    abox_k: int | None = None,
    use_reactive_abox_discovery: bool | None = None,
    corrections: int | None = None,
) -> QueryPipelineResult:
    """Answer one natural-language question using one ontology package.

    Runtime defaults come from `app.core.config`. `model`, `endpoint`, `k`,
    `chunking`, and `corrections` are per-call overrides for the LLM model,
    SPARQL query endpoint, retrieval depth, retrieval index strategy, and
    correction loop limit. Package settings provide package infrastructure such
    as the query endpoint, not experiment defaults. The function writes one
    trace entry to `logs/query.log` and returns the same runtime state in
    structured form.
    """
    root = resolve_package_dir(package_dir)
    metadata = read_json_file(metadata_path(root))
    ontology_context = read_json_file(ontology_context_path(root))
    settings_payload = read_json_file(settings_path(root))

    effective_model = model or settings.default_llm_model
    effective_endpoint = endpoint or _string_setting(
        settings_payload,
        "query_endpoint",
        _string_setting(metadata, "query_endpoint", ""),
    )
    effective_k = k or settings.runtime_retrieval_top_k
    effective_abox_k = abox_k or settings.runtime_abox_retrieval_top_k
    effective_chunking = chunking or settings.default_chunking_strategy
    effective_use_abox_rag = (
        use_abox_rag if use_abox_rag is not None else settings.default_use_abox_rag
    )
    effective_use_reactive_abox_discovery = (
        use_reactive_abox_discovery
        if use_reactive_abox_discovery is not None
        else settings.default_use_reactive_abox_discovery
    )
    max_iterations = corrections or settings.correction_max_iterations

    retrieved_context = retrieve_context(
        root,
        question,
        k=effective_k,
        chunking=effective_chunking,
    )
    retrieved_abox_context = (
        retrieve_abox_context(root, question, k=effective_abox_k)
        if effective_use_abox_rag
        else []
    )
    retrieved_payload = [item.to_dict() for item in retrieved_context]
    retrieved_abox_payload = [item.to_dict() for item in retrieved_abox_context]
    few_shot_examples = retrieve_few_shot_examples(root, question, n=3)
    prompt = render_query_generation_prompt(
        question=question,
        retrieved_context=retrieved_context,
        retrieved_abox_context=retrieved_abox_context,
        metadata=metadata,
        ontology_context=ontology_context,
        few_shot_examples=few_shot_examples,
    )
    attempt_result = await run_query_attempts(
        question=question,
        generation_prompt=prompt,
        retrieved_context=retrieved_context,
        retrieved_abox_context=retrieved_abox_context,
        ontology_context=ontology_context,
        endpoint_url=effective_endpoint,
        model=effective_model,
        llm_api_url=settings.llm_api_url,
        k_max=max_iterations,
        use_reactive_abox_discovery=effective_use_reactive_abox_discovery,
    )

    run_at = datetime.now(UTC)
    run_id = _run_id(run_at, query_readable_runs_dir(root))
    trace_payload = {
        "run_id": run_id,
        "run_at": run_at.strftime("%Y-%m-%dT%H:%MZ"),
        "question_asked": question,
        "dataset_name": _dataset_name(metadata, root.name),
        "dataset_endpoint": effective_endpoint,
        "model_name": effective_model,
        "llm_api_url": settings.llm_api_url,
        "chunking_strategy": effective_chunking,
        "retrieval_top_k": effective_k,
        "abox_retrieval_top_k": effective_abox_k,
        "use_abox_rag": effective_use_abox_rag,
        "use_reactive_abox_discovery": effective_use_reactive_abox_discovery,
        "correction_max_iterations": max_iterations,
        "retrieved_context": retrieved_payload,
        "retrieved_abox_context": retrieved_abox_payload,
        "prompt_generated": prompt,
        "llm_generated_query": attempt_result.original_query,
        "max_correction_iterations": max_iterations,
        "correction_iterations": attempt_result.iterations,
        "corrected_sparql": attempt_result.corrected_query,
        "validated_sparql": attempt_result.validated_query,
        "final_query": attempt_result.final_query,
        "execution_result": attempt_result.execution_result,
        "status": attempt_result.status,
        "errors": attempt_result.errors,
    }
    trace_path = write_query_trace(query_log_path(root), trace_payload)
    readable_trace_path = write_readable_query_trace(
        latest_path=query_readable_latest_path(root),
        runs_dir=query_readable_runs_dir(root),
        run_id=run_id,
        payload=trace_payload,
    )

    return QueryPipelineResult(
        question=question,
        dataset_name=_dataset_name(metadata, root.name),
        dataset_endpoint=effective_endpoint,
        model_name=effective_model,
        retrieved_context=retrieved_payload,
        retrieved_abox_context=retrieved_abox_payload,
        chunking_strategy=effective_chunking,
        retrieval_top_k=effective_k,
        abox_retrieval_top_k=effective_abox_k,
        use_abox_rag=effective_use_abox_rag,
        use_reactive_abox_discovery=effective_use_reactive_abox_discovery,
        correction_max_iterations=max_iterations,
        generated_sparql=attempt_result.original_query,
        validated_sparql=attempt_result.validated_query,
        corrected_sparql=attempt_result.corrected_query,
        execution_result=attempt_result.execution_result,
        status=attempt_result.status,
        errors=attempt_result.errors,
        trace_path=str(trace_path),
        readable_trace_path=str(readable_trace_path),
    )


@dataclass(frozen=True)
class QueryAttemptResult:
    """Final state produced by the candidate-query attempt loop.

    `original_query` is the first LLM candidate. `corrected_query` is the last
    correction candidate, if any correction was requested. `validated_query` is
    set only when a candidate passes formal validation and endpoint execution
    succeeds. `iterations` is the trace-ready attempt log stored under
    `correction_iterations` for backward-compatible trace shape.
    """

    original_query: str
    final_query: str
    validated_query: str | None
    corrected_query: str | None
    execution_result: dict[str, object] | None
    status: str
    errors: list[str] | None
    iterations: list[dict[str, object]]


async def run_query_attempts(
    *,
    question: str,
    generation_prompt: str,
    retrieved_context: list[RetrievedChunk],
    retrieved_abox_context: list[RetrievedABoxChunk] | None = None,
    ontology_context: dict[str, object],
    endpoint_url: str,
    model: str,
    llm_api_url: str,
    k_max: int = 3,
    use_reactive_abox_discovery: bool = False,
) -> QueryAttemptResult:
    """Run the generate -> validate -> execute -> correct loop.

    The first candidate is generated from `generation_prompt`. Each attempt runs
    formal validation against `ontology_context`; valid candidates are executed
    against `endpoint_url`. Validation failures and execution errors are passed
    to the correction helper to produce the next candidate until one succeeds or
    `k_max` attempts have been recorded.
    """
    generated_query, message_history = await query_generation.generate_initial_query_chat(
        generation_prompt,
        model=model,
        llm_api_url=llm_api_url,
        system_role=SYSTEM_ROLE,
    )
    current_query = generated_query
    corrected_query = None
    execution_result = None
    status = "failed"
    errors: list[str] | None = None
    final_query = generated_query
    validated_query = None
    iterations: list[dict[str, object]] = []

    for iteration in range(1, max(1, k_max) + 1):
        validation_result = validate_query(current_query, ontology_context=ontology_context)
        iteration_payload: dict[str, object] = {
            "iteration": iteration,
            "status": "validation_failed",
            "query": current_query,
            "validation": validation_result.to_dict(),
            "validation_summary": _validation_summary(validation_result.to_dict()),
            "errors": validation_result.errors,
            "execution": None,
        }

        execution_stage: ValidationStageResult | None = None
        if validation_result.is_valid:
            try:
                execution_result = await sparql_execution.execute_sparql_query(
                    endpoint_url,
                    validation_result.normalized_query,
                )
                execution_stage = sparql_execution.execution_stage_result()
                validated_query = validation_result.normalized_query
                final_query = validation_result.normalized_query

                # Check for empty results on SELECT queries
                is_empty = _is_empty_select_result(execution_result, validation_result.normalized_query)

                if is_empty and iteration < max(1, k_max):
                    path_hint = ""
                    if use_reactive_abox_discovery:
                        path_hint = await discover_paths_for_correction(
                            endpoint_url,
                            validation_result.normalized_query,
                            ontology_context,
                        )
                    base_message = (
                        "Query executed successfully but returned 0 results. "
                        "Common causes: (1) a concrete entity was matched through the wrong "
                        "name/identifier property, (2) an instance URI was guessed instead of "
                        "using a provided candidate URI, (3) a property path is reversed or "
                        "close but not exact, or (4) a literal datatype/value does not match "
                        "the data."
                    )
                    if path_hint:
                        errors = [base_message + "\n\n" + path_hint]
                    else:
                        errors = [
                            base_message
                            + " Re-read the schema chunks and matching instance candidates carefully."
                        ]
                    status = "completed"
                    iteration_payload["status"] = "executed_empty"
                    iteration_payload["errors"] = errors
                    iteration_payload["execution"] = execution_stage.to_dict()
                else:
                    # Non-empty result or last iteration - accept the result
                    status = "completed"
                    errors = None
                    iteration_payload["status"] = "completed"
                    iteration_payload["errors"] = []
                    iteration_payload["execution"] = execution_stage.to_dict()
                    iterations.append(iteration_payload)
                    break
            except Exception as exc:
                execution_stage = sparql_execution.execution_stage_result(exc)
                errors = [execution_stage.message or execution_stage.code]
                iteration_payload["status"] = "execution_failed"
                iteration_payload["errors"] = errors
        else:
            errors = validation_result.errors

        if execution_stage is not None:
            iteration_payload["execution"] = execution_stage.to_dict()
        iterations.append(iteration_payload)

        if iteration >= max(1, k_max):
            final_query = validation_result.normalized_query
            break

        correction_message = _build_correction_message(errors or [])
        current_query, message_history = await query_correction.correct_query_chat(
            message_history=message_history,
            correction_message=correction_message,
            model=model,
            llm_api_url=llm_api_url,
        )
        corrected_query = current_query

    return QueryAttemptResult(
        original_query=generated_query,
        final_query=final_query,
        validated_query=validated_query,
        corrected_query=corrected_query,
        execution_result=execution_result,
        status=status,
        errors=errors,
        iterations=iterations,
    )


def _string_setting(payload: dict[str, object], key: str, default: str) -> str:
    value = payload.get(key)
    return value if isinstance(value, str) and value.strip() else default


def _dataset_name(metadata: dict[str, object], fallback: str) -> str:
    value = metadata.get("dataset_name")
    if isinstance(value, str) and value.strip():
        return value
    name = metadata.get("ontology_name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return fallback


def _run_id(run_at: datetime, runs_dir: Path) -> str:
    stem = run_at.strftime("%Y%m%d-%H%M")
    candidate = stem
    index = 2
    while (runs_dir / f"{candidate}.txt").exists():
        candidate = f"{stem}-{index}"
        index += 1
    return candidate


def _validation_summary(validation: dict[str, object]) -> str:
    stages = validation.get("stages")
    if not isinstance(stages, list):
        return "VALIDATION_UNKNOWN"
    failed_codes = [
        str(stage.get("code"))
        for stage in stages
        if isinstance(stage, dict) and not stage.get("passed") and isinstance(stage.get("code"), str)
    ]
    return ", ".join(failed_codes) if failed_codes else "VALIDATION_OK"


def _build_correction_message(errors: list[str]) -> str:
    """Build a concise correction message from validation/execution errors.

    Used as a follow-up user message in the chat conversation. The model already
    sees its previous query in the history, so this only needs to convey what
    went wrong and ask for a fix.
    """
    error_text = "\n".join(f"- {e}" for e in errors)
    return (
        f"Your previous query failed. Errors:\n{error_text}\n\n"
        f"Fix the query based on these errors. "
        f"Return only a corrected SPARQL query, no explanations."
    )


def _is_empty_select_result(execution_result: dict[str, object] | None, query: str) -> bool:
    """Check if a SELECT query returned zero result rows.

    Returns False for ASK/CONSTRUCT/DESCRIBE queries (where empty bindings
    are expected or the result format differs).
    """
    if execution_result is None:
        return False

    # Only trigger for SELECT queries
    query_upper = query.strip().lstrip("PREFIX").strip()
    # Find the actual query form after prefix declarations
    for line in query.splitlines():
        stripped = line.strip().upper()
        if stripped and not stripped.startswith("PREFIX"):
            if not stripped.startswith("SELECT"):
                return False
            break

    bindings = execution_result.get("results", {})
    if isinstance(bindings, dict):
        rows = bindings.get("bindings", [])
        if isinstance(rows, list):
            return len(rows) == 0
    return False
