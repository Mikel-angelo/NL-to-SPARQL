"""CLI entry point for ontology-package runtime querying.

The command loads one prepared ontology package, retrieves relevant context for
the question, generates SPARQL, validates it, and executes it.
"""

from __future__ import annotations

import argparse
import asyncio

from app.core.config import settings
from app.domain.package import PackageNotFoundError, get_active_package
from app.domain.rag import SUPPORTED_CHUNKING_ORDER
from app.domain.runtime import run_query_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query a prepared ontology package.")
    parser.add_argument("--question", required=True, help="Natural-language question")
    parser.add_argument("--model", help="Optional model override")
    parser.add_argument("--k", type=int, help="Optional retrieval top-k override")
    parser.add_argument("--corrections", type=int, help="Optional correction attempt limit")
    parser.add_argument(
        "--chunking",
        choices=SUPPORTED_CHUNKING_ORDER,
        help="Optional retrieval index strategy override",
    )
    return parser.parse_args()


async def main() -> None:
    """Run the runtime pipeline and print the high-signal result fields."""
    args = parse_args()
    try:
        package_dir = get_active_package(settings.ontology_packages_path)
    except PackageNotFoundError as exc:
        raise SystemExit(str(exc)) from exc

    print(f"Using active ontology package: {package_dir}")

    result = await run_query_pipeline(
        args.question,
        package_dir,
        model=args.model,
        k=args.k,
        chunking=args.chunking,
        corrections=args.corrections,
    )
    print(f"Chunking: {result.chunking_strategy}")
    print(f"Retrieval top-k: {result.retrieval_top_k}")
    print(f"Correction attempts max: {result.correction_max_iterations}")
    print(f"Answer: {result.execution_result}")
    print(f"Generated SPARQL:\n{result.generated_sparql or ''}")
    print(f"Trace: {result.trace_path}")
    print(f"Readable trace: {result.readable_trace_path}")
    print(f"Status: {result.status}")
    if result.errors:
        print(f"Errors: {result.errors}")


if __name__ == "__main__":
    asyncio.run(main())
