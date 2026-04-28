"""Prepare ontology package files from a file or SPARQL endpoint.

This is the extraction step inside onboarding: it validates/parses the source,
builds the final RDF graph, calls the context builder, and writes JSON/package
artifacts. The root-level `onboard.py` file is only the CLI wrapper.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
from io import BytesIO

import httpx
from rdflib import Graph

from app.core.config import settings
from app.domain.package import (
    DomainError,
    ensure_package_dirs,
    logs_dir,
    metadata_path,
    ontology_context_path,
    schemas_dir,
    settings_path,
    source_path,
    write_json_file,
)
from app.domain.ontology.context_builder import extract_ontology_context
from app.domain.ontology.schema_resolution import (
    CoverageResult,
    DetectionResult,
    ResolvedSchemaFile,
    SchemaResolutionResult,
    analyze_schema_coverage,
    build_final_graph,
    classify_mode,
    detect_graph,
    parse_uploaded_content,
    resolve_schemas_for_namespaces,
)


SUPPORTED_SUFFIXES = {".ttl", ".owl", ".rdf"}


class OntologyExtractionError(DomainError):
    """Raised when ontology extraction fails."""


@dataclass(frozen=True)
class ExtractionResult:
    """Structured output from ontology extraction."""

    package_dir: Path
    metadata: dict[str, object]
    ontology_context: dict[str, object]
    settings: dict[str, object]
    source_mode: str
    source_path: Path | None
    sparql_endpoint: str | None
    resolved_schemas: list[ResolvedSchemaFile]


async def extract_metadata(
    source: str,
    package_dir: str | Path,
    *,
    source_mode: str = "file",
    source_filename_override: str | None = None,
    dataset_name: str | None = None,
    query_endpoint: str | None = None,
    default_model: str | None = None,
    chunking: str = "class_based",
) -> ExtractionResult:
    """Extract metadata/context from a file path or SPARQL endpoint into an ontology package."""
    root = ensure_package_dirs(package_dir)

    if source_mode == "file":
        return await _extract_from_file(
            source=source,
            package_dir=root,
            source_filename_override=source_filename_override,
            dataset_name=dataset_name,
            query_endpoint=query_endpoint,
            default_model=default_model,
            chunking=chunking,
        )

    if source_mode == "sparql_endpoint":
        return await _extract_from_sparql_endpoint(
            endpoint=source,
            package_dir=root,
            query_endpoint=query_endpoint or source,
            default_model=default_model,
            chunking=chunking,
        )

    raise OntologyExtractionError(f"Unsupported source mode: {source_mode}")


async def _extract_from_file(
    *,
    source: str,
    package_dir: Path,
    source_filename_override: str | None,
    dataset_name: str | None,
    query_endpoint: str | None,
    default_model: str | None,
    chunking: str,
) -> ExtractionResult:
    """Build package artifacts from a local ontology file."""
    source_path = Path(source).resolve()
    if not source_path.exists():
        raise OntologyExtractionError(f"Ontology file not found: {source_path.as_posix()}")

    suffix = source_path.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise OntologyExtractionError("Only .ttl, .owl, and .rdf files are supported")

    content = source_path.read_bytes()
    if not content:
        raise OntologyExtractionError("Ontology file is empty")

    initial_graph = await parse_uploaded_content(content, suffix)
    detection = detect_graph(initial_graph)
    mode = classify_mode(detection)
    coverage = analyze_schema_coverage(initial_graph)
    resolved = await resolve_schemas_for_namespaces(coverage.missing_namespaces)
    final_graph = build_final_graph(initial_graph, resolved.resolved_files)

    source_filename = source_filename_override or source_path.name
    ontology_name = _slugify_filename(source_filename)
    ontology_context = extract_ontology_context(
        final_graph,
        ontology_name=ontology_name,
        source_filename=source_filename,
    )

    copied_source_path = source_path_for_package(package_dir, suffix)
    copied_source_path.write_bytes(content)
    _write_resolved_schemas(package_dir, resolved.resolved_files)

    timestamp = datetime.now(UTC)
    metadata = _build_metadata(
        attempted_at=timestamp,
        ontology_name=ontology_name,
        source_filename=source_filename,
        ontology_file=copied_source_path.relative_to(package_dir).as_posix(),
        dataset_name=dataset_name,
        query_endpoint=query_endpoint,
        mode=mode,
        detection=detection,
        coverage=coverage,
        initial_graph=initial_graph,
        final_graph=final_graph,
        resolved=resolved,
        source_mode="file",
    )
    write_json_file(metadata_path(package_dir), metadata)
    write_json_file(ontology_context_path(package_dir), ontology_context)

    settings_payload = _settings_payload(
        source_mode="file",
        query_endpoint=query_endpoint,
        dataset_name=dataset_name,
        default_model=default_model,
        chunking=chunking,
    )
    write_json_file(settings_path(package_dir), settings_payload)
    _append_log(logs_dir(package_dir) / "onboard.log", "metadata_extracted", metadata=metadata)

    return ExtractionResult(
        package_dir=package_dir,
        metadata=metadata,
        ontology_context=ontology_context,
        settings=settings_payload,
        source_mode="file",
        source_path=copied_source_path,
        sparql_endpoint=query_endpoint,
        resolved_schemas=resolved.resolved_files,
    )


async def _extract_from_sparql_endpoint(
    *,
    endpoint: str,
    package_dir: Path,
    query_endpoint: str,
    default_model: str | None,
    chunking: str,
) -> ExtractionResult:
    """Build package artifacts from an existing SPARQL endpoint."""
    graph = await _graph_from_sparql_endpoint(endpoint)
    detection = detect_graph(graph)
    mode = classify_mode(detection)
    coverage = analyze_schema_coverage(graph)
    final_graph = build_final_graph(graph, [])

    ontology_name = _slugify_endpoint(endpoint)
    ontology_context = extract_ontology_context(
        final_graph,
        ontology_name=ontology_name,
        source_filename=endpoint,
    )

    timestamp = datetime.now(UTC)
    metadata = _build_metadata(
        attempted_at=timestamp,
        ontology_name=ontology_name,
        source_filename=endpoint,
        ontology_file=None,
        dataset_name=None,
        query_endpoint=query_endpoint,
        mode=mode,
        detection=detection,
        coverage=coverage,
        initial_graph=graph,
        final_graph=final_graph,
        resolved=SchemaResolutionResult(resolved_files=[], attempted_urls=[], failed_urls=[]),
        source_mode="sparql_endpoint",
    )
    write_json_file(metadata_path(package_dir), metadata)
    write_json_file(ontology_context_path(package_dir), ontology_context)

    settings_payload = _settings_payload(
        source_mode="sparql_endpoint",
        query_endpoint=query_endpoint,
        dataset_name=None,
        default_model=default_model,
        chunking=chunking,
    )
    write_json_file(settings_path(package_dir), settings_payload)
    _append_log(logs_dir(package_dir) / "onboard.log", "metadata_extracted", metadata=metadata)

    return ExtractionResult(
        package_dir=package_dir,
        metadata=metadata,
        ontology_context=ontology_context,
        settings=settings_payload,
        source_mode="sparql_endpoint",
        source_path=None,
        sparql_endpoint=query_endpoint,
        resolved_schemas=[],
    )


def _build_metadata(
    *,
    attempted_at: datetime,
    ontology_name: str,
    source_filename: str,
    ontology_file: str | None,
    dataset_name: str | None,
    query_endpoint: str | None,
    mode: str,
    detection: DetectionResult,
    coverage: CoverageResult,
    initial_graph: Graph,
    final_graph: Graph,
    resolved: SchemaResolutionResult,
    source_mode: str,
) -> dict[str, object]:
    files_loaded = []
    if ontology_file:
        files_loaded.append(ontology_file)
    files_loaded.extend(f"ontology/schemas/{schema_file.filename}" for schema_file in resolved.resolved_files)

    metadata: dict[str, object] = {
        "loaded_at": attempted_at.isoformat(),
        "ontology_name": ontology_name,
        "source_filename": source_filename,
        "source_mode": source_mode,
        "dataset_name": dataset_name,
        "query_endpoint": query_endpoint,
        "ontology_file": ontology_file,
        "mode": mode,
        "initial_graph": {
            "triple_count": len(initial_graph),
            "classes_count": detection.classes_count,
            "instances_count": detection.instances_count,
            "properties_count": detection.properties_count,
        },
        "schema_coverage": {
            "status": coverage.status,
            "missing_class_uris": coverage.missing_class_uris,
            "missing_namespaces": coverage.missing_namespaces,
        },
        "files_loaded": files_loaded,
        "resolved_schemas": [
            {
                "source_namespace": schema_file.source_namespace,
                "url": schema_file.url,
                "local_file": f"ontology/schemas/{schema_file.filename}",
            }
            for schema_file in resolved.resolved_files
        ],
        "triple_count": len(final_graph),
    }
    return metadata


def _write_resolved_schemas(package_dir: Path, schema_files: list[ResolvedSchemaFile]) -> None:
    for schema_file in schema_files:
        (schemas_dir(package_dir) / schema_file.filename).write_bytes(schema_file.content)


def source_path_for_package(package_dir: Path, suffix: str) -> Path:
    return source_path(package_dir, suffix)


def _settings_payload(
    *,
    source_mode: str,
    query_endpoint: str | None,
    dataset_name: str | None,
    default_model: str | None,
    chunking: str,
) -> dict[str, object]:
    return {
        "source_mode": source_mode,
        "query_endpoint": query_endpoint,
        "dataset_name": dataset_name,
        "default_model": default_model or settings.default_llm_model,
        "chunking_strategy": chunking,
        "retrieval_top_k": settings.runtime_retrieval_top_k,
        "correction_max_iterations": settings.correction_max_iterations,
        "llm_api_url": settings.llm_api_url,
    }


def _append_log(path: Path, event: str, **details: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp": datetime.now(UTC).isoformat(),
        "event": event,
        **details,
    }
    with path.open("a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(entry))
        log_file.write("\n")


async def _graph_from_sparql_endpoint(endpoint: str) -> Graph:
    query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
    headers = {
        "Accept": "text/turtle, application/rdf+xml, application/n-triples, text/plain",
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(endpoint, data={"query": query}, headers=headers)
            response.raise_for_status()
    except httpx.HTTPError as exc:
        raise OntologyExtractionError(f"Failed to read triples from SPARQL endpoint: {endpoint}") from exc

    graph = Graph()
    parse_errors: list[Exception] = []
    for rdf_format in _candidate_graph_formats(response.headers.get("content-type", "")):
        try:
            graph.parse(source=BytesIO(response.content), format=rdf_format)
            if len(graph) > 0:
                return graph
        except Exception as exc:  # pragma: no cover - fallback loop
            parse_errors.append(exc)

    raise OntologyExtractionError(
        f"Unable to parse RDF graph returned by SPARQL endpoint: {endpoint}"
    ) from (parse_errors[-1] if parse_errors else None)


def _candidate_graph_formats(content_type: str) -> list[str]:
    lowered = content_type.lower()
    if "text/turtle" in lowered:
        return ["turtle", "xml", "nt"]
    if "rdf+xml" in lowered or "application/xml" in lowered or "text/xml" in lowered:
        return ["xml", "turtle", "nt"]
    if "n-triples" in lowered or "plain" in lowered:
        return ["nt", "turtle", "xml"]
    return ["turtle", "xml", "nt"]


def _slugify_filename(filename: str) -> str:
    stem = Path(filename).stem.lower()
    return "-".join(part for part in stem.replace("_", "-").split("-") if part) or "ontology"


def _slugify_endpoint(endpoint: str) -> str:
    text = endpoint.rstrip("/")
    tail = text.rsplit("/", 1)[-1]
    return _slugify_filename(tail or "endpoint")
