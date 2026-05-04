"""CLI entry point for direct ontology-package evaluation.

This file owns argument parsing, matching the other root-level CLI commands.
The evaluation package owns the actual experiment execution and artifact
writing.
"""

from __future__ import annotations

import argparse
import asyncio

import httpx

from app.domain.package import DomainError
from app.domain.rag import SUPPORTED_CHUNKING_ORDER
from evaluation.experiment_runner import run_from_cli


def parse_args() -> argparse.Namespace:
    """Parse CLI flags for evaluation."""
    parser = argparse.ArgumentParser(description="Evaluate a prepared ontology package.")
    parser.add_argument("--dataset", required=True, help="Evaluation dataset JSON path")
    parser.add_argument("--package", required=True, help="Active package directory path or name")
    parser.add_argument("--model", default="", help="Optional model override")
    parser.add_argument("--k", type=int, default=None, help="Optional retrieval top-k override")
    parser.add_argument(
        "--chunking",
        choices=SUPPORTED_CHUNKING_ORDER,
        default=None,
        help="Optional retrieval index strategy override",
    )
    parser.add_argument("--corrections", type=int, default=None, help="Optional correction attempt limit")
    parser.add_argument("--output", default="", help="Optional output directory. Defaults to <package>/evaluation/<run-id>/")
    parser.add_argument("--preflight-timeout", type=float, default=30.0, help="Endpoint preflight timeout in seconds")
    return parser.parse_args()


async def main() -> None:
    """Parse CLI arguments and run evaluation."""
    try:
        await run_from_cli(parse_args())
    except (DomainError, httpx.HTTPError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    asyncio.run(main())
