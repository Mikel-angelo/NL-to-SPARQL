"""Run the ontology-package runtime query pipeline.

Runtime starts once an ontology package already exists. This module retrieves
context for a question, renders the prompt, validates/corrects the result, and
executes the generated SPARQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json

from app.core.config import settings
from app.domain.package import (
    metadata_path,
    ontology_context_path,
    query_log_path,
    read_json_file,
    resolve_package_dir,
    settings_path,
)
from app.domain.rag import RetrievedChunk, retrieve_context
from app.domain.runtime.correction_loop import generate_with_correction
from app.domain.runtime.prompt_renderer import render_query_generation_prompt


@dataclass(frozen=True)
class QueryPipelineResult:
    """Structured query-pipeline output."""

    question: str
    dataset_name: str
    dataset_endpoint: str
    retrieved_context: list[dict[str, object]]
    generated_sparql: str | None
    validated_sparql: str | None
    corrected_sparql: str | None
    execution_result: dict[str, object] | None
    status: str
    errors: list[str] | None
    trace_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "question": self.question,
            "dataset_name": self.dataset_name,
            "dataset_endpoint": self.dataset_endpoint,
            "retrieved_context": self.retrieved_context,
            "generated_sparql": self.generated_sparql,
            "validated_sparql": self.validated_sparql,
            "corrected_sparql": self.corrected_sparql,
            "execution_result": self.execution_result,
            "status": self.status,
            "errors": self.errors,
            "trace_path": self.trace_path,
        }


async def run_query_pipeline(
    question: str,
    package_dir: str | Path,
    *,
    model: str | None = None,
    endpoint: str | None = None,
    k: int | None = None,
) -> QueryPipelineResult:
    """Run retrieve -> generate -> validate/correct -> execute using one ontology package."""
    root = resolve_package_dir(package_dir)
    metadata = read_json_file(metadata_path(root))
    ontology_context = read_json_file(ontology_context_path(root))
    settings_payload = read_json_file(settings_path(root))

    effective_model = model or _string_setting(settings_payload, "default_model", settings.default_llm_model)
    effective_endpoint = endpoint or _string_setting(
        settings_payload,
        "query_endpoint",
        _string_setting(metadata, "query_endpoint", ""),
    )
    effective_k = k or _int_setting(settings_payload, "retrieval_top_k", settings.runtime_retrieval_top_k)
    max_iterations = _int_setting(settings_payload, "correction_max_iterations", settings.correction_max_iterations)

    retrieved_context = retrieve_context(
        root,
        question,
        k=effective_k,
    )
    retrieved_payload = [item.to_dict() for item in retrieved_context]
    # Retrieval is the first runtime step because it depends on the question.
    prompt = render_query_generation_prompt(
        question=question,
        retrieved_context=retrieved_context,
        metadata=metadata,
        ontology_context=ontology_context,
    )
    correction_result = await generate_with_correction(
        question=question,
        initial_prompt=prompt,
        ontology_context=ontology_context,
        endpoint_url=effective_endpoint,
        model=effective_model,
        llm_api_url=_llm_api_url(settings_payload),
        k_max=max_iterations,
    )

    trace_payload = {
        "run_at": datetime.now(UTC).isoformat(),
        "question_asked": question,
        "dataset_name": _dataset_name(metadata, root.name),
        "dataset_endpoint": effective_endpoint,
        "retrieved_context": retrieved_payload,
        "prompt_generated": prompt,
        "llm_generated_query": correction_result.original_query,
        "max_correction_iterations": max_iterations,
        "correction_iterations": correction_result.iterations,
        "corrected_sparql": correction_result.corrected_query,
        "validated_sparql": correction_result.validated_query,
        "final_query": correction_result.final_query,
        "execution_result": correction_result.execution_result,
        "status": correction_result.status,
        "errors": correction_result.errors,
    }
    trace_path = write_query_trace(query_log_path(root), trace_payload)

    return QueryPipelineResult(
        question=question,
        dataset_name=_dataset_name(metadata, root.name),
        dataset_endpoint=effective_endpoint,
        retrieved_context=retrieved_payload,
        generated_sparql=correction_result.original_query,
        validated_sparql=correction_result.validated_query,
        corrected_sparql=correction_result.corrected_query,
        execution_result=correction_result.execution_result,
        status=correction_result.status,
        errors=correction_result.errors,
        trace_path=str(trace_path),
    )


def write_query_trace(path: Path, payload: dict[str, object]) -> Path:
    """Append the latest trace and also persist a readable JSON array."""
    existing: list[dict[str, object]] = []
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, list):
                existing = [item for item in loaded if isinstance(item, dict)]
        except (OSError, json.JSONDecodeError):
            existing = []
    existing.append(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    return path


def _string_setting(payload: dict[str, object], key: str, default: str) -> str:
    value = payload.get(key)
    return value if isinstance(value, str) and value.strip() else default


def _int_setting(payload: dict[str, object], key: str, default: int) -> int:
    value = payload.get(key)
    return int(value) if isinstance(value, (int, float)) else default


def _llm_api_url(settings_payload: dict[str, object]) -> str:
    """Read the generic LLM URL, with backward compatibility for older packages."""
    return _string_setting(
        settings_payload,
        "llm_api_url",
        _string_setting(settings_payload, "ollama_url", settings.llm_api_url),
    )


def _dataset_name(metadata: dict[str, object], fallback: str) -> str:
    value = metadata.get("dataset_name")
    if isinstance(value, str) and value.strip():
        return value
    name = metadata.get("ontology_name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return fallback
