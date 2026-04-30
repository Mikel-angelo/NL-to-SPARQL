"""CLI entry point for ontology onboarding and package generation.

This command prepares one ontology package. In file mode it also provisions the
Fuseki dataset after metadata extraction and index construction complete.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from app.core.config import settings
from app.clients.fuseki import FusekiService
from app.domain.ontology import onboard_ontology_file, onboard_sparql_endpoint


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Onboard an ontology into a reusable ontology package.")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--ontology", help="Path to an ontology file (.owl, .ttl, .rdf)")
    source_group.add_argument("--sparql-endpoint", help="Existing SPARQL query endpoint to inspect")
    parser.add_argument("--output", required=True, help="Output ontology packages directory")
    parser.add_argument("--name", help="Optional package/dataset name base. A minute timestamp is appended.")
    parser.add_argument("--model", help="Default model to save in settings.json")
    parser.add_argument(
        "--chunking",
        default="class_based",
        choices=["class_based", "property_based", "composite"],
        help="Default retrieval index strategy saved in settings.json. All supported indexes are built.",
    )
    return parser.parse_args()


async def main() -> None:
    """Parse CLI flags, build one ontology package, and optionally provision Fuseki."""
    args = parse_args()
    fuseki_service = FusekiService()
    output_root = Path(args.output).resolve()

    if args.ontology:
        result = await onboard_ontology_file(
            args.ontology,
            packages_root=output_root,
            fuseki_service=fuseki_service,
            source_filename=Path(args.ontology).name,
            package_name=args.name,
            default_model=args.model,
            chunking=args.chunking,
            status_callback=_print_status,
        )
        print(f"Ontology package: {result.package_dir}")
        print(f"Dataset name: {result.dataset_name}")
        print(f"Dataset endpoint: {result.dataset_endpoint}")
        print(f"Query endpoint: {result.query_endpoint}")
        print(f"Artifacts: {result.chunks_path} | {result.index_path}")
        return

    result = await onboard_sparql_endpoint(
        args.sparql_endpoint,
        packages_root=output_root,
        default_model=args.model,
        chunking=args.chunking,
        package_name=args.name,
        status_callback=_print_status,
    )
    print(f"Ontology package: {result.package_dir}")
    print(f"SPARQL endpoint: {result.query_endpoint}")
    print(f"Artifacts: {result.chunks_path} | {result.index_path}")

def _print_status(event: str, **details: object) -> None:
    """Print high-signal onboarding stages so the CLI shows progress."""
    if event == "loading_source":
        print(f"[1/4] Loading source and extracting ontology context from {details.get('source')}")
    elif event == "building_indexes":
        print(f"[2/4] Building retrieval indexes (default: {details.get('default_chunking')})")
    elif event == "uploading_to_fuseki":
        print(f"[3/4] Uploading dataset to Fuseki: {details.get('dataset_name')}")
    elif event == "package_activated":
        print(f"[4/4] Activated ontology package: {details.get('activated_package')}")
    elif event == "onboarding_completed":
        print(f"Completed with {details.get('chunk_count')} chunks")


if __name__ == "__main__":
    asyncio.run(main())
