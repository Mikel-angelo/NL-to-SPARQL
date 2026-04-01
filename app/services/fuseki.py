from dataclasses import dataclass

import httpx
from fastapi import HTTPException, status

from app.core.config import settings


@dataclass(frozen=True)
class FusekiUploadPayload:
    dataset_name: str
    content: bytes
    content_type: str


class FusekiService:
    def __init__(self) -> None:
        self._base_url = settings.fuseki_base_url.rstrip("/")
        self._auth = (
            settings.fuseki_admin_username,
            settings.fuseki_admin_password,
        )

    async def create_dataset(self, dataset_name: str) -> None:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self._base_url}/$/datasets",
                params={"dbType": "tdb2", "dbName": dataset_name},
                auth=self._auth,
            )

        if response.status_code >= 400:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to create Fuseki dataset: {response.text}",
            )

    async def upload_rdf(self, payload: FusekiUploadPayload) -> None:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{self._base_url}/{payload.dataset_name}/data",
                params={"default": ""},
                content=payload.content,
                headers={"Content-Type": payload.content_type},
                auth=self._auth,
            )

        if response.status_code >= 400:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to upload RDF to Fuseki: {response.text}",
            )
