"""CLI entry point for direct ontology-package evaluation."""

from __future__ import annotations

import asyncio

from evaluation.experiment_runner import parse_args, run_from_cli


async def main() -> None:
    """Parse CLI arguments and run evaluation."""
    await run_from_cli(parse_args())


if __name__ == "__main__":
    asyncio.run(main())
