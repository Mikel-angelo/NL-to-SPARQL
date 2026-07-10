"""Thin LLM transport helper.

The runtime pipeline owns prompt construction and output validation. This
module only sends a prompt to the configured LLM endpoint and returns text.

Two modes:
- `generate_text` — single-turn via /api/generate (legacy)
- `chat_completion` — multi-turn via /api/chat, used by the correction loop so
  the model retains conversational context across correction iterations.
"""

from __future__ import annotations

import httpx

from app.core.config import settings


async def generate_text(prompt: str, *, model: str, llm_api_url: str) -> str:
    """Send one prompt to the configured LLM endpoint and return raw text."""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": settings.llm_temperature,
            "num_ctx": settings.llm_num_ctx,
            "num_predict": settings.llm_num_predict,
        },
    }

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds) as client:
        response = await client.post(llm_api_url, json=payload)
        response.raise_for_status()

    body = response.json()
    generated = body.get("response")
    if not isinstance(generated, str):
        raise ValueError("The LLM response is missing generated text")
    return generated


async def chat_completion(
    messages: list[dict[str, str]],
    *,
    model: str,
    llm_api_url: str,
) -> tuple[str, list[dict[str, str]]]:
    """Send a conversation to the chat endpoint and return (text, updated_messages).

    Uses the Ollama /api/chat endpoint which maintains multi-turn context. The
    returned `updated_messages` includes the new assistant response appended,
    ready for the next correction turn.

    The chat URL is derived from `llm_api_url` by replacing /api/generate with
    /api/chat.
    """
    chat_url = _chat_url(llm_api_url)
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": settings.llm_temperature,
            "num_ctx": settings.llm_num_ctx,
            "num_predict": settings.llm_num_predict,
        },
    }

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds) as client:
        response = await client.post(chat_url, json=payload)
        response.raise_for_status()

    body = response.json()
    message = body.get("message", {})
    content = message.get("content", "")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("The LLM chat response is missing content")

    updated_messages = messages + [{"role": "assistant", "content": content}]
    return content, updated_messages


def _chat_url(llm_api_url: str) -> str:
    """Derive the chat endpoint URL from the generate endpoint URL."""
    if "/api/generate" in llm_api_url:
        return llm_api_url.replace("/api/generate", "/api/chat")
    base = llm_api_url.rstrip("/")
    if base.endswith("/api"):
        return base + "/chat"
    return base + "/api/chat"
