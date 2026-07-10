"""Generate and normalize initial SPARQL candidates.

This module owns the first LLM call in the runtime flow. It receives the fully
rendered generation prompt, calls the configured LLM endpoint, and normalizes the
raw model output into plain SPARQL.

Two modes:
- `generate_initial_query` — single-turn via /api/generate (legacy)
- `generate_initial_query_chat` — multi-turn via /api/chat, returns the message
  history so the correction loop can continue the same conversation.
"""

from __future__ import annotations

import re

from app.clients.llm import chat_completion, generate_text


async def generate_initial_query(prompt: str, *, model: str, llm_api_url: str) -> str:
    """Return the first normalized SPARQL candidate from an LLM prompt."""
    return normalize_generated_query(
        await generate_text(prompt, model=model, llm_api_url=llm_api_url)
    )


async def generate_initial_query_chat(
    prompt: str,
    *,
    model: str,
    llm_api_url: str,
    system_role: str = "",
) -> tuple[str, list[dict[str, str]]]:
    """Generate initial SPARQL via chat API, returning (query, message_history).

    The message history contains the system message, the user prompt, and the
    assistant response. The correction loop appends to this history to maintain
    full conversational context across iterations.
    """
    messages: list[dict[str, str]] = []
    if system_role:
        messages.append({"role": "system", "content": system_role})
    messages.append({"role": "user", "content": prompt})

    raw_response, updated_messages = await chat_completion(
        messages, model=model, llm_api_url=llm_api_url
    )
    query = normalize_generated_query(raw_response)
    return query, updated_messages


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
