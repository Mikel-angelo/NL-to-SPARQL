"""
Manual smoke test for query-generation prompt rendering.

This script:
- loads the current ontology metadata from `storage/current/metadata.json`
- loads the current retrieval chunks from `storage/current/class_chunks.json`
- takes the top configured retrieval chunks as a simple prompt context sample
- renders the full prompt that would be sent to the LLM
- prints that prompt to stdout

It does not call the LLM. It is only a quick way to inspect the final prompt.
"""

import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings
from app.services.runtime.query_generation import QueryGenerationService


def main() -> None:
    metadata_path = Path("storage/current/metadata.json")
    chunks_path = Path("storage/current/class_chunks.json")

    if not metadata_path.exists():
        print(f"Metadata file not found: {metadata_path.resolve()}")
        raise SystemExit(1)

    if not chunks_path.exists():
        print(f"Chunks file not found: {chunks_path.resolve()}")
        raise SystemExit(1)

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    class_chunks = json.loads(chunks_path.read_text(encoding="utf-8"))

    if not isinstance(class_chunks, list):
        print("class_chunks.json has an invalid format.")
        raise SystemExit(1)

    retrieved_context = []
    for rank, chunk in enumerate(class_chunks[: settings.runtime_retrieval_top_k], start=1):
        if not isinstance(chunk, dict):
            continue
        retrieved_context.append(
            {
                "rank": rank,
                "class_name": chunk.get("class_name"),
                "class_uri": chunk.get("class_uri"),
                "text": chunk.get("text"),
                "metadata": chunk.get("metadata"),
            }
        )

    question = "What types of actors exist in scenarios?"
    service = QueryGenerationService()
    prompt = service.render_prompt(
        question=question,
        retrieved_context=retrieved_context,
        metadata=metadata,
    )

    print("Query generation prompt rendered successfully.\n")
    print("Question:", question)
    print("Retrieved chunks:", len(retrieved_context))
    print("\nPrompt:\n")
    print(prompt)


if __name__ == "__main__":
    main()
