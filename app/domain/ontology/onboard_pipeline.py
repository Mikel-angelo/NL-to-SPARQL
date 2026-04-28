"""Coordinate the full ontology onboarding pipeline.

This domain module is called by both the root-level CLI `onboard.py` and the
HTTP API. It runs extraction, index building, optional Fuseki upload, and active
package activation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json

from app.clients.fuseki import FusekiService, FusekiUploadPayload
from app.domain.ontology.onboarding_extraction import ExtractionResult, extract_metadata
from app.domain.package import get_active_package, set_active_package
from app.domain.rag import build_index


SUPPORTED_SUFFIXES = {".ttl", ".owl", ".rdf"}


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
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S-%f")
    dataset_name = f"{ontology_name}-{timestamp}"
    package_dir = Path(packages_root).resolve() / dataset_name
    query_endpoint = f"{fuseki_service.dataset_endpoint(dataset_name)}/query"

    _emit_status(status_callback, package_dir, "extracting_metadata", source=str(source_file))
    extraction = await extract_metadata(
        str(source_file),
        package_dir,
        source_mode="file",
        source_filename_override=effective_source_filename,
        dataset_name=dataset_name,
        query_endpoint=query_endpoint,
        default_model=default_model,
        chunking=chunking,
    )

    _emit_status(status_callback, package_dir, "building_index", chunking=chunking)
    artifact_result = build_index(package_dir, chunking=chunking)

    _emit_status(status_callback, package_dir, "uploading_to_fuseki", dataset_name=dataset_name)
    uploads = _build_fuseki_uploads(
        dataset_name=dataset_name,
        source_filename=effective_source_filename,
        extraction=extraction,
    )
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
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S-%f")
    package_dir = Path(packages_root).resolve() / f"{_slugify_endpoint(endpoint)}-{timestamp}"

    _emit_status(status_callback, package_dir, "extracting_metadata", source=endpoint)
    await extract_metadata(
        endpoint,
        package_dir,
        source_mode="sparql_endpoint",
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


def _build_fuseki_uploads(
    *,
    dataset_name: str,
    source_filename: str,
    extraction: ExtractionResult,
) -> list[FusekiUploadPayload]:
    if extraction.source_path is None:
        raise ValueError("Expected a copied ontology source file in the ontology package")

    uploads = [
        FusekiUploadPayload(
            dataset_name=dataset_name,
            filename=source_filename,
            content=extraction.source_path.read_bytes(),
        )
    ]
    uploads.extend(
        FusekiUploadPayload(
            dataset_name=dataset_name,
            filename=schema_file.filename,
            content=schema_file.content,
        )
        for schema_file in extraction.resolved_schemas
    )
    return uploads


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
    log_path = package_dir / "logs" / "onboard.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp": datetime.now(UTC).isoformat(),
        "event": event,
        **details,
    }
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry))
        handle.write("\n")
