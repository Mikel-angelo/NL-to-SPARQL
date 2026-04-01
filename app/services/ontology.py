import re
from datetime import datetime, UTC
from pathlib import Path

from fastapi import HTTPException, UploadFile, status

from app.core.config import settings
from app.services.fuseki import FusekiService, FusekiUploadPayload


CONTENT_TYPES = {
    ".ttl": "text/turtle",
    ".rdf": "application/rdf+xml",
    ".owl": "application/rdf+xml",
}


class OntologyService:
    def __init__(self) -> None:
        self._storage_path = Path(settings.ontology_storage_path)
        self._fuseki_service = FusekiService()

    async def upload_ontology(self, file: UploadFile) -> tuple[str, str]:
        filename = file.filename or "ontology"
        suffix = Path(filename).suffix.lower()
        content_type = CONTENT_TYPES.get(suffix)
        if content_type is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Unsupported ontology file type",
            )
        base_name = self._slugify_filename(filename)
        timestamp_suffix = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        ontology_id = f"{base_name}-{timestamp_suffix}"
        dataset_name = ontology_id

        content = await file.read()
        if not content:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Uploaded file is empty",
            )

        self._storage_path.mkdir(parents=True, exist_ok=True)
        file_path = self._storage_path / f"{ontology_id}{suffix}"
        file_path.write_bytes(content)

        try:
            await self._fuseki_service.create_dataset(dataset_name)
            await self._fuseki_service.upload_rdf(
                FusekiUploadPayload(
                    dataset_name=dataset_name,
                    content=content,
                    content_type=content_type,
                )
            )
        except Exception:
            if file_path.exists():
                file_path.unlink()
            raise

        return ontology_id, dataset_name

    @staticmethod
    def _slugify_filename(filename: str) -> str:
        stem = Path(filename).stem.lower()
        slug = re.sub(r"[^a-z0-9]+", "-", stem).strip("-")
        if not slug:
            slug = "ontology"
        return slug[:50].rstrip("-")
