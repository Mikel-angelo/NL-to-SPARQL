"""
Bring one uploaded ontology into the framework.

Responsibilities:
- accept one ontology source (.owl, .ttl, .rdf)
- validate file format and file content
- assign a dataset name from filename + timestamp
- orchestrate parsing, detection, classification, and optional schema resolution
- trigger ontology context extraction
- store the current ontology artifacts locally
- create and replace the Fuseki dataset
- upload all loaded RDF files to Fuseki
- record onboarding progress in the current load log

Outputs:
- dataset_name
- endpoint
- current metadata.json
- current ontology_context.json
- current load.log
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
from app.services.onboarding.ontology_context import OntologyContextService
from app.services.onboarding.rag_index_service import RAGIndexService
from app.services.onboarding.ontology_schema_resolution import (
    CoverageResult,
    DetectionResult,
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
        rag_index_service: RAGIndexService | None = None,
        storage_dir: Path | None = None,
    ) -> None:
        self._fuseki_service = fuseki_service or FusekiService()
        self._ontology_schema_resolution_service = (
            ontology_schema_resolution_service or OntologySchemaResolutionService()
        )
        self._ontology_context_service = ontology_context_service or OntologyContextService()
        self._storage_dir = storage_dir or Path(settings.storage_path)
        self._rag_index_service = rag_index_service or RAGIndexService(storage_dir=self._storage_dir)
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

            coverage = self._ontology_schema_resolution_service.analyze_schema_coverage(initial_graph)
            load_log.log(
                "coverage_analyzed",
                status=coverage.status,
                instance_type_count=len(coverage.instance_type_uris),
                declared_class_count=len(coverage.declared_class_uris),
                missing_class_count=len(coverage.missing_class_uris),
                missing_namespaces=coverage.missing_namespaces,
            )
            initial_metadata = self._build_metadata(
                attempted_at=attempted_at,
                upload=upload,
                mode=mode,
                detection=detection,
                coverage=coverage,
                initial_graph=initial_graph,
            )
            self._current_dir.mkdir(parents=True, exist_ok=True)
            (self._current_dir / upload.ontology_filename).write_bytes(upload.content)
            (self._current_dir / "metadata.json").write_text(
                json.dumps(initial_metadata, indent=2),
                encoding="utf-8",
            )
            load_log.flush()

            resolved = SchemaResolutionResult(
                resolved_files=[],
                attempted_urls=[],
                failed_urls=[],
            )
            if coverage.missing_namespaces:
                load_log.log("schema_resolution_started")
                resolved = await self._ontology_schema_resolution_service.resolve_schemas_for_namespaces(
                    coverage.missing_namespaces
                )
                load_log.log(
                    "schema_resolution_result",
                    resolved_schemas=len(resolved.resolved_files),
                    missing_class_uris=coverage.missing_class_uris,
                    attempted_urls=resolved.attempted_urls,
                    failed_urls=resolved.failed_urls,
                )

            final_graph = self._ontology_schema_resolution_service.build_final_graph(
                initial_graph,
                resolved.resolved_files,
            )
            if resolved.resolved_files:
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
                detection=detection,
                coverage=coverage,
                initial_graph=initial_graph,
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

            # TODO: consider moving RAG index creation to a separate endpoint that can be called after onboarding completes, to avoid adding latency to the critical path of getting the ontology into Fuseki and available for querying
            class_chunks = self._rag_index_service.create_class_chunks(ontology_context)
            load_log.log("class_chunks_created", chunk_count=len(class_chunks))
            self._save_class_chunks_metadata(metadata, len(class_chunks))
            load_log.log(
                "class_chunks_saved",
                chunk_count=len(class_chunks),
                path="storage/current/class_chunks.json",
            )
            texts = self._rag_index_service.extract_texts(class_chunks)
            load_log.log("class_chunk_texts_extracted", text_count=len(texts))
            vectors = self._rag_index_service.embed_chunks(texts)
            load_log.log("class_chunk_embeddings_created", vector_count=len(vectors))
            vector_index = self._rag_index_service.build_vector_index(vectors)
            load_log.log("vector_index_created", entry_count=vector_index.ntotal)
            index_path = self._rag_index_service.save_vector_index(vector_index)
            self._save_vector_index_metadata(metadata, vector_index.ntotal, index_path.name)
            load_log.log("vector_index_saved", entry_count=vector_index.ntotal, path=f"storage/current/{index_path.name}")
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
        detection: DetectionResult,
        coverage: CoverageResult,
        initial_graph: Graph,
        final_graph: Graph | None = None,
        resolved: SchemaResolutionResult | None = None,
    ) -> dict[str, object]:
        """Build the lightweight runtime metadata saved next to the ontology."""
        resolved_files = resolved.resolved_files if resolved else []
        files_loaded = [upload.ontology_filename] + [
            f"schemas/{schema_file.filename}" for schema_file in resolved_files
        ]
        metadata = {
            "loaded_at": attempted_at.isoformat(),
            "ontology_name": upload.ontology_name,
            "source_filename": upload.source_filename,
            "dataset_name": upload.dataset_name,
            "endpoint": upload.endpoint,
            "ontology_file": upload.ontology_filename,
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
                    "local_file": f"schemas/{schema_file.filename}",
                }
                for schema_file in resolved_files
            ],
        }
        if final_graph is not None:
            metadata["triple_count"] = len(final_graph)
        return metadata

    def _save_class_chunks_metadata(self, metadata: dict[str, object], chunk_count: int) -> None:
        """Update the current metadata file after class chunk generation completes."""
        files_loaded = metadata.setdefault("files_loaded", [])
        if isinstance(files_loaded, list) and "class_chunks.json" not in files_loaded:
            files_loaded.append("class_chunks.json")
        metadata["class_chunks"] = {
            "count": chunk_count,
            "file": "class_chunks.json",
        }
        (self._current_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2),
            encoding="utf-8",
        )

    def _save_vector_index_metadata(self, metadata: dict[str, object], entry_count: int, filename: str) -> None:
        """Update the current metadata file after vector index generation completes."""
        files_loaded = metadata.setdefault("files_loaded", [])
        if isinstance(files_loaded, list) and filename not in files_loaded:
            files_loaded.append(filename)
        metadata["vector_index"] = {
            "entries": entry_count,
            "file": filename,
            "embedding_model": settings.rag_embedding_model_name,
        }
        (self._current_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2),
            encoding="utf-8",
        )

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
