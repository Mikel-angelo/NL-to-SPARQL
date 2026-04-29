"""Generate and normalize initial SPARQL candidates.

This module owns the first LLM call in the runtime flow. It receives the fully
rendered generation prompt, calls the configured LLM endpoint, and normalizes the
raw model output into plain SPARQL. It does not know about ontology packages,
validation rules, endpoint execution, correction attempts, or trace writing.
"""

from __future__ import annotations

import re

from app.clients.llm import generate_text


async def generate_initial_query(prompt: str, *, model: str, llm_api_url: str) -> str:
    """Return the first normalized SPARQL candidate from an LLM prompt.

    The caller is responsible for rendering the prompt and deciding what to do
    with the candidate. This function only calls the LLM and strips common
    response wrapping such as markdown fences.
    """
    return normalize_generated_query(
        await generate_text(prompt, model=model, llm_api_url=llm_api_url)
    )


def normalize_generated_query(generated_text: str) -> str:
    """Normalize raw LLM output into a SPARQL query string.

    The prompt asks the LLM to return only SPARQL, but this accepts fenced code
    blocks as a practical cleanup step. Empty results are treated as generation
    failures and raise `ValueError`.
    """
    text = generated_text.strip()
    if text.startswith("```"):
        fenced_match = re.match(r"^```[A-Za-z0-9_-]*\s*(.*?)```$", text, re.DOTALL)
        if fenced_match:
            text = fenced_match.group(1).strip()
    if not text:
        raise ValueError("The LLM returned an empty query")
    return text
