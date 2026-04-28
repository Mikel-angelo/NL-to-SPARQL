"""LLM-backed SPARQL self-correction loop."""

from __future__ import annotations

from dataclasses import dataclass
import re

import httpx

from app.clients.llm import generate_text
from app.core.config import settings
from app.domain.runtime.prompt_renderer import render_correction_prompt
from app.domain.runtime.validation import ValidationStageResult, execution_stage_result, validate_query


@dataclass(frozen=True)
class CorrectionLoopResult:
    """Final query and full iteration log from the self-correction loop."""

    original_query: str
    final_query: str
    validated_query: str | None
    corrected_query: str | None
    execution_result: dict[str, object] | None
    status: str
    errors: list[str] | None
    iterations: list[dict[str, object]]


async def generate_with_correction(
    *,
    question: str,
    initial_prompt: str,
    ontology_context: dict[str, object],
    endpoint_url: str,
    model: str,
    llm_api_url: str,
    k_max: int = 3,
) -> CorrectionLoopResult:
    """Generate SPARQL, validate/execute it, and retry with feedback on failure."""
    generated_query = normalize_generated_query(
        await generate_text(initial_prompt, model=model, llm_api_url=llm_api_url)
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
            "query": current_query,
            "validation": validation_result.to_dict(),
            "execution": None,
        }

        execution_stage: ValidationStageResult | None = None
        if validation_result.is_valid:
            try:
                execution_result = await execute_sparql_query(endpoint_url, validation_result.normalized_query)
                execution_stage = execution_stage_result()
                status = "completed"
                errors = None
                validated_query = validation_result.normalized_query
                final_query = validation_result.normalized_query
                iteration_payload["execution"] = execution_stage.to_dict()
                iterations.append(iteration_payload)
                break
            except Exception as exc:
                execution_stage = execution_stage_result(exc)
                errors = [execution_stage.message or execution_stage.code]
        else:
            errors = validation_result.errors

        if execution_stage is not None:
            iteration_payload["execution"] = execution_stage.to_dict()
        iterations.append(iteration_payload)

        if iteration >= max(1, k_max):
            final_query = validation_result.normalized_query
            break

        correction_prompt = render_correction_prompt(
            original_question=question,
            failed_query=current_query,
            validation_errors=errors or [],
        )
        current_query = normalize_generated_query(
            await generate_text(correction_prompt, model=model, llm_api_url=llm_api_url)
        )
        corrected_query = current_query

    return CorrectionLoopResult(
        original_query=generated_query,
        final_query=final_query,
        validated_query=validated_query,
        corrected_query=corrected_query,
        execution_result=execution_result,
        status=status,
        errors=errors,
        iterations=iterations,
    )


async def execute_sparql_query(endpoint: str, query: str) -> dict[str, object]:
    """Execute SPARQL against an explicit endpoint URL."""
    if not endpoint:
        raise ValueError("No SPARQL query endpoint is configured")

    async with httpx.AsyncClient(timeout=settings.fuseki_upload_timeout_seconds) as client:
        response = await client.post(
            endpoint,
            data={"query": query},
            headers={"Accept": "application/sparql-results+json, application/json"},
        )
        response.raise_for_status()
    return response.json()


def normalize_generated_query(generated_text: str) -> str:
    """Normalize raw LLM output into SPARQL."""
    text = generated_text.strip()
    if text.startswith("```"):
        fenced_match = re.match(r"^```[A-Za-z0-9_-]*\s*(.*?)```$", text, re.DOTALL)
        if fenced_match:
            text = fenced_match.group(1).strip()
    if not text:
        raise ValueError("The LLM returned an empty query")
    return text
