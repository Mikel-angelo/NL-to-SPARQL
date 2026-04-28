"""Thin LLM transport helper.

The runtime pipeline owns prompt construction and output validation. This
module only sends a prompt to the configured LLM endpoint and returns text.
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
