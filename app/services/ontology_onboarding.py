"""
Responsible for bringing a new ontology into the framework.

Functions:
    • accept ontology source (.owl, .ttl, .rdf)
    • validate file format and file content
    • assign dataset name from filename + timestamp
    • orchestrate parsing, detection, classification, and optional schema resolution
    • trigger ontology context extraction
    • store current ontology artifacts locally
    • create and replace the Fuseki dataset
    • upload all loaded RDF files to Fuseki
    • record onboarding progress in the current load log

Outputs:
    • dataset_name
    • endpoint_url
    • current metadata.json
    • current ontology_context.json
    • current load.log
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree
from time import perf_counter
import json

from fastapi import HTTPException, UploadFile, status
from rdflib import Graph

from app.core.config import settings
from app.services.fuseki import FusekiService, FusekiUploadPayload
from app.services.ontology_context import OntologyContextService
from app.services.ontology_schema_resolution import (
    OntologySchemaResolutionService,
    ResolvedSchemaFile,
    SchemaResolutionResult,
)


SUPPORTED_SUFFIXES = {".ttl", ".owl", ".rdf"}


@dataclass(frozen=True)
class PreparedOntologyUpload:
    """Validated ontology upload data used by the onboarding flow."""

    source_filename: str
    ontology_name: str
    dataset_name: str
    endpoint: str
    ontology_filename: str
    content: bytes
    suffix: str


class OntologyOnboardingService:
    """Bring one ontology into the framework through the onboarding pipeline."""

    def __init__(
        self,
        fuseki_service: FusekiService | None = None,
        ontology_schema_resolution_service: OntologySchemaResolutionService | None = None,
        ontology_context_service: OntologyContextService | None = None,
        storage_dir: Path | None = None,
    ) -> None:
        self._fuseki_service = fuseki_service or FusekiService()
        self._ontology_schema_resolution_service = (
            ontology_schema_resolution_service or OntologySchemaResolutionService()
        )
        self._ontology_context_service = ontology_context_service or OntologyContextService()
        self._storage_dir = storage_dir or Path(settings.storage_path)
        self._current_dir = self._storage_dir / "current"
        self._schemas_dir = self._current_dir / "schemas"

    async def load_ontology(self, file: UploadFile) -> dict[str, str]:
        """Run the full ontology onboarding pipeline for one uploaded file."""
        started_at = perf_counter()
        attempted_at = self._now()
        load_log = LoadLog(self._current_dir / "load.log")

        try:
            upload = await self._prepare_upload(file, attempted_at)
            previous_dataset_name = self._get_current_dataset_name()
            load_log.log(
                "upload_received",
                source_filename=upload.source_filename,
                dataset_name=upload.dataset_name,
                bytes=len(upload.content),
            )

            initial_graph = await self._ontology_schema_resolution_service.parse_uploaded_content(
                upload.content,
                upload.suffix,
            )
            load_log.log(
                "parse_completed",
                triple_count=len(initial_graph),
            )

            detection = self._ontology_schema_resolution_service.detect(initial_graph)
            load_log.log(
                "detection_completed",
                classes_count=detection.classes_count,
                properties_count=detection.properties_count,
                instances_count=detection.instances_count,
            )

            mode = self._ontology_schema_resolution_service.classify_mode(detection)
            load_log.log("mode_detected", mode=mode)

            resolved = SchemaResolutionResult(
                resolved_files=[],
                attempted_urls=[],
                failed_urls=[],
            )
            if mode == "instances-only":
                load_log.log("schema_resolution_started")
                resolved = await self._ontology_schema_resolution_service.resolve_schemas(initial_graph)
                load_log.log(
                    "schema_resolution_result",
                    resolved_schemas=len(resolved.resolved_files),
                    attempted_urls=resolved.attempted_urls,
                    failed_urls=resolved.failed_urls,
                )

            final_graph = self._ontology_schema_resolution_service.build_final_graph(
                initial_graph,
                resolved.resolved_files,
            )
            if mode == "instances-only":
                load_log.log(
                    "schemas_added_to_graph",
                    added_schemas=len(resolved.resolved_files),
                )
            load_log.log(
                "final_graph_ready",
                triple_count=len(final_graph),
            )

            ontology_context = self._ontology_context_service.extract_context(
                final_graph,
                ontology_name=upload.ontology_name,
                source_filename=upload.source_filename,
            )
            load_log.log("context_extraction_completed")

            metadata = self._build_metadata(
                attempted_at=attempted_at,
                upload=upload,
                mode=mode,
                final_graph=final_graph,
                resolved=resolved,
            )
            self._save_current(
                upload=upload,
                metadata=metadata,
                ontology_context=ontology_context,
                resolved_schemas=resolved.resolved_files,
                load_log=load_log,
            )
            load_log.log("artifacts_saved", files_loaded=metadata["files_loaded"])

            upload_files = self._build_fuseki_uploads(upload, resolved.resolved_files)
            await self._fuseki_service.replace_dataset(
                dataset_name=upload.dataset_name,
                files=upload_files,
                previous_dataset_name=previous_dataset_name,
            )
            load_log.log("fuseki_loaded", uploaded_files=len(upload_files))
            load_log.log("onboarding_completed", elapsed_seconds=round(perf_counter() - started_at, 2))
            return {
                "dataset_name": upload.dataset_name,
                "endpoint": upload.endpoint,
            }
        except Exception as exc:
            load_log.log(
                "onboarding_failed",
                elapsed_seconds=round(perf_counter() - started_at, 2),
                error=str(exc),
            )
            raise

    def _save_current(
        self,
        upload: PreparedOntologyUpload,
        metadata: dict[str, object],
        ontology_context: dict[str, object],
        resolved_schemas: list[ResolvedSchemaFile],
        load_log: "LoadLog",
    ) -> None:
        """Replace the current ontology artifacts on disk for the active run."""
        self._current_dir.mkdir(parents=True, exist_ok=True)
        self._clear_current()
        (self._current_dir / upload.ontology_filename).write_bytes(upload.content)
        (self._current_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2),
            encoding="utf-8",
        )
        (self._current_dir / "ontology_context.json").write_text(
            json.dumps(ontology_context, indent=2),
            encoding="utf-8",
        )

        if resolved_schemas:
            self._schemas_dir.mkdir(parents=True, exist_ok=True)
            for schema_file in resolved_schemas:
                (self._schemas_dir / schema_file.filename).write_bytes(schema_file.content)

        load_log.flush()

    def _build_metadata(
        self,
        attempted_at: datetime,
        upload: PreparedOntologyUpload,
        mode: str,
        final_graph: Graph,
        resolved: SchemaResolutionResult,
    ) -> dict[str, object]:
        """Build the lightweight runtime metadata saved next to the ontology."""
        files_loaded = [upload.ontology_filename] + [
            f"schemas/{schema_file.filename}" for schema_file in resolved.resolved_files
        ]
        return {
            "loaded_at": attempted_at.isoformat(),
            "ontology_name": upload.ontology_name,
            "source_filename": upload.source_filename,
            "dataset_name": upload.dataset_name,
            "endpoint": upload.endpoint,
            "ontology_file": upload.ontology_filename,
            "mode": mode,
            "triple_count": len(final_graph),
            "files_loaded": files_loaded,
            "resolved_schemas": [
                {
                    "source_namespace": schema_file.source_namespace,
                    "url": schema_file.url,
                    "local_file": f"schemas/{schema_file.filename}",
                }
                for schema_file in resolved.resolved_files
            ],
        }

    def _build_fuseki_uploads(
        self,
        upload: PreparedOntologyUpload,
        resolved_schemas: list[ResolvedSchemaFile],
    ) -> list[FusekiUploadPayload]:
        """Build all file uploads that must land in the single Fuseki dataset."""
        uploads = [
            FusekiUploadPayload(
                dataset_name=upload.dataset_name,
                filename=upload.source_filename,
                content=upload.content,
            )
        ]
        uploads.extend(
            FusekiUploadPayload(
                dataset_name=upload.dataset_name,
                filename=schema_file.filename,
                content=schema_file.content,
            )
            for schema_file in resolved_schemas
        )
        return uploads

    def _get_current_dataset_name(self) -> str | None:
        """Return the dataset name currently stored in local runtime metadata."""
        metadata_path = self._current_dir / "metadata.json"
        if not metadata_path.exists():
            return None
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        return metadata.get("dataset_name")

    def _clear_current(self) -> None:
        """Delete the previously stored current ontology artifacts."""
        if not self._current_dir.exists():
            return
        for path in self._current_dir.iterdir():
            if path.is_dir():
                rmtree(path)
            else:
                path.unlink()

    async def _prepare_upload(
        self,
        file: UploadFile,
        attempted_at: datetime,
    ) -> PreparedOntologyUpload:
        """Validate the uploaded file and build dataset naming."""
        source_filename = file.filename or "ontology"
        suffix = Path(source_filename).suffix.lower()
        if suffix not in SUPPORTED_SUFFIXES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only .ttl, .owl, and .rdf files are supported",
            )

        content = await file.read()
        if not content:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Uploaded file is empty",
            )

        ontology_name = self._slugify_filename(source_filename)
        dataset_name = f"{ontology_name}-{attempted_at.strftime('%Y%m%d-%H%M%S-%f')}"
        return PreparedOntologyUpload(
            source_filename=source_filename,
            ontology_name=ontology_name,
            dataset_name=dataset_name,
            endpoint=self._fuseki_service.dataset_endpoint(dataset_name),
            ontology_filename=f"ontology{suffix}",
            content=content,
            suffix=suffix,
        )

    @staticmethod
    def _slugify_filename(filename: str) -> str:
        """Build a dataset-safe ontology name from the uploaded filename."""
        stem = Path(filename).stem.lower()
        return "-".join(part for part in stem.replace("_", "-").split("-") if part) or "ontology"

    @staticmethod
    def _now() -> datetime:
        """Return the current UTC timestamp used for dataset naming."""
        return datetime.now(UTC)


class LoadLog:
    """Write structured onboarding events to the current runtime log file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: list[dict[str, object]] = []
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, event: str, **details: object) -> None:
        """Append one structured event to the load log and persist immediately."""
        self._entries.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "event": event,
                **details,
            }
        )
        self.flush()

    def flush(self) -> None:
        """Write the full log as JSON lines."""
        lines = [json.dumps(entry) for entry in self._entries]
        self._path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
