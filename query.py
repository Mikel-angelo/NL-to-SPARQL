"""CLI entry point for ontology-package runtime querying.

The command loads one prepared ontology package, retrieves relevant context for
the question, generates SPARQL, validates it, and executes it.
"""

from __future__ import annotations

import argparse
import asyncio

from app.core.config import settings
from app.domain.package import PackageNotFoundError, get_active_package
from app.domain.runtime import run_query_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query a prepared ontology package.")
    parser.add_argument(
        "--package",
        help="Ontology package directory. If omitted, uses the active package from ontology_packages/.active_package.",
    )
    parser.add_argument("--question", required=True, help="Natural-language question")
    parser.add_argument("--model", help="Optional model override")
    parser.add_argument("--endpoint", help="Optional SPARQL query endpoint override")
    parser.add_argument("--k", type=int, help="Optional retrieval top-k override")
    return parser.parse_args()


async def main() -> None:
    """Run the runtime pipeline and print the high-signal result fields."""
    args = parse_args()
    try:
        package_dir = args.package or get_active_package(settings.ontology_packages_path)
    except PackageNotFoundError as exc:
        raise SystemExit(str(exc)) from exc

    if not args.package:
        print(f"Using active ontology package: {package_dir}")

    result = await run_query_pipeline(
        args.question,
        package_dir,
        model=args.model,
        endpoint=args.endpoint,
        k=args.k,
    )
    print(f"Answer: {result.execution_result}")
    print(f"Generated SPARQL:\n{result.generated_sparql or ''}")
    print(f"Trace: {result.trace_path}")
    print(f"Status: {result.status}")
    if result.errors:
        print(f"Errors: {result.errors}")


if __name__ == "__main__":
    asyncio.run(main())
