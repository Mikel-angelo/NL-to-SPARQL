from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
import json

from fastapi import HTTPException, UploadFile, status

from app.core.config import settings
from app.services.fuseki import FusekiService
from app.services.ontology_context import OntologyContextService


CONTENT_TYPES = {
    ".ttl": "text/turtle",
    ".owl": "application/rdf+xml",
    ".rdf": "application/rdf+xml",
}


@dataclass(frozen=True)
class PreparedOntologyUpload:
    """Validated ontology upload data used by the onboarding flow."""

    source_filename: str
    ontology_name: str
    dataset_name: str
    endpoint: str
    ontology_filename: str
    content: bytes


class OntologyOnboardingService:
    """Loads one ontology file into Fuseki and stores the current local metadata."""

    def __init__(
        self,
        fuseki_service: FusekiService | None = None,
        ontology_context_service: OntologyContextService | None = None,
        storage_dir: Path | None = None,
    ) -> None:
        self._fuseki_service = fuseki_service or FusekiService()
        self._ontology_context_service = ontology_context_service or OntologyContextService()
        self._storage_dir = storage_dir or Path(settings.storage_path)
        self._current_dir = self._storage_dir / "current"

    async def load_ontology(self, file: UploadFile) -> dict[str, str]:
        """Loads an ontology file, uploads it to Fuseki, and writes current files."""
        load_log = LoadLog(self._current_dir / "load.log")
        started_at = perf_counter()
        attempted_at = self._now()
        try:
            load_log.write("Starting ontology onboarding")
            upload = await self._prepare_upload(file, attempted_at, load_log)
            load_log.write(
                f"Prepared upload for '{upload.source_filename}' as dataset '{upload.dataset_name}'"
            )

            load_log.write("Replacing Fuseki dataset")
            await self._fuseki_service.replace_dataset(
                dataset_name=upload.dataset_name,
                filename=upload.source_filename,
                previous_dataset_name=self._get_current_dataset_name(),
                content=upload.content,
            )
            load_log.write("Fuseki dataset upload completed")

            load_log.write("Extracting ontology context")
            ontology_context = self._ontology_context_service.extract_from_content(
                content=upload.content,
                suffix=Path(upload.ontology_filename).suffix.lower(),
                ontology_name=upload.ontology_name,
                source_filename=upload.source_filename,
            )
            load_log.write("Ontology context extracted")
            metadata = {
                "loaded_at": attempted_at.isoformat(),
                "ontology_name": upload.ontology_name,
                "source_filename": upload.source_filename,
                "dataset_name": upload.dataset_name,
                "endpoint": upload.endpoint,
                "ontology_file": upload.ontology_filename,
            }
            load_log.write("Writing current ontology files")
            self._save_current(
                ontology_filename=upload.ontology_filename,
                ontology_content=upload.content,
                metadata=metadata,
                ontology_context=ontology_context,
                load_log=load_log,
            )
            load_log.write(
                f"Onboarding completed in {perf_counter() - started_at:.2f}s"
            )
            load_log.flush()

            return {
                "dataset_name": upload.dataset_name,
                "endpoint": upload.endpoint,
            }
        except Exception as exc:
            load_log.write(
                f"Onboarding failed after {perf_counter() - started_at:.2f}s: {exc}"
            )
            raise

    def _save_current(
        self,
        ontology_filename: str,
        ontology_content: bytes,
        metadata: dict[str, object],
        ontology_context: dict[str, object],
        load_log: "LoadLog",
    ) -> None:
        """Replaces the current ontology artifact, metadata, and ontology context on disk."""
        self._current_dir.mkdir(parents=True, exist_ok=True)
        self._clear_current()
        (self._current_dir / ontology_filename).write_bytes(ontology_content)
        (self._current_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2),
            encoding="utf-8",
        )
        (self._current_dir / "ontology_context.json").write_text(
            json.dumps(ontology_context, indent=2),
            encoding="utf-8",
        )
        load_log.flush()

    def _get_current_dataset_name(self) -> str | None:
        """Returns the dataset name of the currently stored ontology when present."""
        metadata_path = self._current_dir / "metadata.json"
        if not metadata_path.exists():
            return None
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        return metadata.get("dataset_name")

    def _clear_current(self) -> None:
        """Deletes the previous current ontology files before writing the new ones."""
        for path in self._current_dir.iterdir():
            path.unlink()

    async def _prepare_upload(
        self,
        file: UploadFile,
        attempted_at: datetime,
        load_log: "LoadLog",
    ) -> PreparedOntologyUpload:
        """Validates the uploaded file and builds the dataset naming."""
        source_filename = file.filename or "ontology"
        ontology_name = self._slugify_filename(source_filename)
        dataset_name = f"{ontology_name}-{attempted_at.strftime('%Y%m%d-%H%M%S-%f')}"
        endpoint = self._fuseki_service.dataset_endpoint(dataset_name)

        suffix = Path(source_filename).suffix.lower()
        if CONTENT_TYPES.get(suffix) is None:
            load_log.write(f"Rejected unsupported file format: {suffix or '<none>'}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only .ttl, .owl, and .rdf files are supported",
            )

        content = await file.read()
        if not content:
            load_log.write("Rejected empty uploaded file")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Uploaded file is empty",
            )
        load_log.write(f"Read {len(content)} bytes from uploaded ontology file")

        return PreparedOntologyUpload(
            source_filename=source_filename,
            ontology_name=ontology_name,
            dataset_name=dataset_name,
            endpoint=endpoint,
            ontology_filename=f"ontology{suffix}",
            content=content,
        )

    @staticmethod
    def _slugify_filename(filename: str) -> str:
        """Builds a safe ontology name from the uploaded filename."""
        stem = Path(filename).stem.lower()
        return "-".join(part for part in stem.replace("_", "-").split("-") if part) or "ontology"

    @staticmethod
    def _now() -> datetime:
        """Returns the current UTC timestamp used for dataset naming."""
        return datetime.now(UTC)


class LoadLog:
    """Stores a step-by-step execution log for the current onboarding run."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: list[str] = []
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, message: str) -> None:
        """Appends a timestamped log line and persists it immediately."""
        timestamp = datetime.now(UTC).isoformat()
        self._entries.append(f"{timestamp} {message}")
        self.flush()

    def flush(self) -> None:
        """Writes the current log content to disk."""
        self._path.write_text("\n".join(self._entries) + "\n", encoding="utf-8")
