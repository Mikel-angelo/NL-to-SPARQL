"""Generate corrected SPARQL candidates after failed attempts.

This module owns one correction action: turn the original question, failed query,
and validation/execution feedback into a correction prompt, call the configured
LLM, and normalize the returned SPARQL. It does not decide whether correction is
needed, how many retries are allowed, or whether a corrected query succeeds.
"""

from __future__ import annotations

from app.clients.llm import generate_text
from app.domain.rag import RetrievedChunk
from app.domain.runtime.prompt_renderer import render_correction_prompt
from app.domain.runtime.query_generation import normalize_generated_query


async def correct_query(
    *,
    question: str,
    failed_query: str,
    validation_errors: list[str],
    retrieved_context: list[RetrievedChunk],
    ontology_context: dict[str, object],
    model: str,
    llm_api_url: str,
) -> str:
    """Return the next normalized SPARQL candidate from correction feedback.

    `validation_errors` also carries endpoint execution errors because the LLM
    needs the same kind of textual feedback regardless of whether the failed
    attempt was rejected before execution or by the SPARQL endpoint.
    """
    correction_prompt = render_correction_prompt(
        original_question=question,
        failed_query=failed_query,
        validation_errors=validation_errors,
        retrieved_context=retrieved_context,
        ontology_context=ontology_context,
    )
    return normalize_generated_query(
        await generate_text(correction_prompt, model=model, llm_api_url=llm_api_url)
    )
