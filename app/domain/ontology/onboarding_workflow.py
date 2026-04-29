"""Run the top-level ontology onboarding workflows.

File onboarding loads RDF, prepares the final graph, builds `ontology_context.json`,
writes package artifacts, builds the RAG index, uploads the package data to Fuseki,
and activates the package. Endpoint onboarding follows the same package/index flow
but uses the existing SPARQL endpoint instead of creating a Fuseki dataset.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json

from app.clients.fuseki import FusekiService
from app.domain.ontology.graph_preparation import prepare_final_graph
from app.domain.ontology.ontology_context import build_ontology_context
from app.domain.ontology.package_activation import build_fuseki_uploads_from_package
from app.domain.ontology.package_writer import (
    OntologyPackageArtifacts,
    append_onboard_log,
    write_ontology_package,
)
from app.domain.ontology.source_loader import SUPPORTED_SUFFIXES, load_ontology_file, load_sparql_endpoint
from app.domain.package import get_active_package, set_active_package
from app.domain.rag import build_index


@dataclass(frozen=True)
class OnboardingResult:
    """Structured result returned by the shared onboarding flow."""

    package_dir: Path
    dataset_name: str | None
    dataset_endpoint: str | None
    query_endpoint: str | None
    source_mode: str
    chunks_path: Path
    index_path: Path
    chunk_count: int


async def onboard_ontology_file(
    source_path: str | Path,
    *,
    packages_root: str | Path,
    fuseki_service: FusekiService,
    source_filename: str | None = None,
    default_model: str | None = None,
    chunking: str = "class_based",
    activate_package: bool = True,
    status_callback: callable | None = None,
) -> OnboardingResult:
    """Build one package from a file, index it, upload it to Fuseki, and optionally activate it."""
    source_file = Path(source_path).resolve()
    suffix = source_file.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise ValueError("Only .ttl, .owl, and .rdf files are supported")

    effective_source_filename = source_filename or source_file.name
    ontology_name = _slugify_filename(effective_source_filename)
    packages_root_path = Path(packages_root).resolve()
    dataset_name = _unique_timestamped_name(packages_root_path, ontology_name)
    package_dir = packages_root_path / dataset_name
    query_endpoint = f"{fuseki_service.dataset_endpoint(dataset_name)}/query"

    _emit_status(status_callback, package_dir, "loading_source", source=str(source_file))
    source = await load_ontology_file(
        str(source_file),
        source_filename=effective_source_filename,
    )
    _emit_status(status_callback, package_dir, "preparing_graph", source=str(source_file))
    final_graph = await prepare_final_graph(source.graph)
    ontology_context = build_ontology_context(
        final_graph.graph,
        ontology_name=ontology_name,
        source_filename=effective_source_filename,
    )
    _emit_status(status_callback, package_dir, "writing_package", source=str(source_file))
    artifacts = write_ontology_package(
        package_dir=package_dir,
        source=source,
        final_graph=final_graph,
        ontology_name=ontology_name,
        ontology_context=ontology_context,
        dataset_name=dataset_name,
        query_endpoint=query_endpoint,
        default_model=default_model,
        chunking=chunking,
    )

    _emit_status(status_callback, package_dir, "building_index", chunking=chunking)
    artifact_result = build_index(package_dir, chunking=chunking)

    _emit_status(status_callback, package_dir, "uploading_to_fuseki", dataset_name=dataset_name)
    uploads = build_fuseki_uploads_from_package(package_dir, dataset_name=dataset_name)
    await fuseki_service.replace_dataset(
        dataset_name=dataset_name,
        files=uploads,
        previous_dataset_name=_previous_dataset_name(packages_root),
    )

    if activate_package:
        set_active_package(packages_root, package_dir)
        _emit_status(status_callback, package_dir, "package_activated", activated_package=str(package_dir))

    _emit_status(
        status_callback,
        package_dir,
        "onboarding_completed",
        completed_package=str(package_dir),
        dataset_name=dataset_name,
        chunk_count=artifact_result.chunk_count,
    )
    return OnboardingResult(
        package_dir=package_dir,
        dataset_name=dataset_name,
        dataset_endpoint=fuseki_service.dataset_endpoint(dataset_name),
        query_endpoint=query_endpoint,
        source_mode="file",
        chunks_path=artifact_result.chunks_path,
        index_path=artifact_result.index_path,
        chunk_count=artifact_result.chunk_count,
    )


async def onboard_sparql_endpoint(
    endpoint: str,
    *,
    packages_root: str | Path,
    default_model: str | None = None,
    chunking: str = "class_based",
    activate_package: bool = True,
    status_callback: callable | None = None,
) -> OnboardingResult:
    """Build one package from an existing SPARQL endpoint and optionally activate it."""
    packages_root_path = Path(packages_root).resolve()
    package_dir = packages_root_path / _unique_timestamped_name(
        packages_root_path,
        _slugify_endpoint(endpoint),
    )

    _emit_status(status_callback, package_dir, "loading_source", source=endpoint)
    source = await load_sparql_endpoint(endpoint)
    _emit_status(status_callback, package_dir, "preparing_graph", source=endpoint)
    final_graph = await prepare_final_graph(source.graph, resolve_missing_schemas=False)
    ontology_name = _slugify_endpoint(endpoint)
    ontology_context = build_ontology_context(
        final_graph.graph,
        ontology_name=ontology_name,
        source_filename=endpoint,
    )
    _emit_status(status_callback, package_dir, "writing_package", source=endpoint)
    write_ontology_package(
        package_dir=package_dir,
        source=source,
        final_graph=final_graph,
        ontology_name=ontology_name,
        ontology_context=ontology_context,
        dataset_name=None,
        query_endpoint=endpoint,
        default_model=default_model,
        chunking=chunking,
    )

    _emit_status(status_callback, package_dir, "building_index", chunking=chunking)
    artifact_result = build_index(package_dir, chunking=chunking)

    if activate_package:
        set_active_package(packages_root, package_dir)
        _emit_status(status_callback, package_dir, "package_activated", activated_package=str(package_dir))

    _emit_status(
        status_callback,
        package_dir,
        "onboarding_completed",
        completed_package=str(package_dir),
        dataset_name=None,
        chunk_count=artifact_result.chunk_count,
    )
    return OnboardingResult(
        package_dir=package_dir,
        dataset_name=None,
        dataset_endpoint=None,
        query_endpoint=endpoint,
        source_mode="sparql_endpoint",
        chunks_path=artifact_result.chunks_path,
        index_path=artifact_result.index_path,
        chunk_count=artifact_result.chunk_count,
    )


def _previous_dataset_name(packages_root: str | Path) -> str | None:
    try:
        active_root = get_active_package(packages_root)
    except Exception:
        return None

    metadata_file = active_root / "metadata.json"
    if not metadata_file.exists():
        return None

    try:
        metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None

    dataset_name = metadata.get("dataset_name")
    return dataset_name if isinstance(dataset_name, str) and dataset_name.strip() else None


def _slugify_filename(filename: str) -> str:
    stem = Path(filename).stem.lower()
    return "-".join(part for part in stem.replace("_", "-").split("-") if part) or "ontology"


def _slugify_endpoint(endpoint: str) -> str:
    text = endpoint.rstrip("/")
    tail = text.rsplit("/", 1)[-1]
    normalized = tail.lower().replace("_", "-")
    return "-".join(part for part in normalized.split("-") if part) or "endpoint"


def _unique_timestamped_name(root: Path, base_name: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M")
    stem = f"{base_name}-{timestamp}"
    candidate = stem
    index = 2
    while (root / candidate).exists():
        candidate = f"{stem}-{index}"
        index += 1
    return candidate


def _emit_status(
    callback: callable | None,
    package_dir: Path,
    event: str,
    **details: object,
) -> None:
    _append_onboard_log(package_dir, event, **details)
    if callback is not None:
        callback(event, **details)


def _append_onboard_log(package_dir: Path, event: str, **details: object) -> None:
    append_onboard_log(package_dir / "logs" / "onboard.log", event, **details)
