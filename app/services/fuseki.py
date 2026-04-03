"""
Responsible for all direct Apache Jena Fuseki interactions.

Functions:
    • create datasets
    • delete datasets
    • upload RDF files into a dataset
    • replace the current dataset with a newly prepared one
    • build the public dataset endpoint URL

Outputs:
    • Fuseki dataset lifecycle changes
    • uploaded RDF content in Fuseki
    • SPARQL query results
    • endpoint_url
"""

from dataclasses import dataclass

import httpx
from fastapi import HTTPException, status

from app.core.config import settings


@dataclass(frozen=True)
class FusekiUploadPayload:
    """Payload used to upload RDF content to a Fuseki dataset."""

    dataset_name: str
    filename: str
    content: bytes


class FusekiService:
    """Wraps the HTTP operations needed to work with Apache Jena Fuseki."""

    def __init__(self) -> None:
        self._base_url = settings.fuseki_base_url.rstrip("/")
        self._auth = (
            settings.fuseki_admin_username,
            settings.fuseki_admin_password,
        )
        self._admin_timeout = settings.fuseki_admin_timeout_seconds
        self._upload_timeout = settings.fuseki_upload_timeout_seconds

    async def create_dataset(self, dataset_name: str) -> None:
        """Creates a new dataset in Fuseki."""
        try:
            async with httpx.AsyncClient(timeout=self._admin_timeout) as client:
                response = await client.post(
                    f"{self._base_url}/$/datasets",
                    params={"dbType": "tdb2", "dbName": dataset_name},
                    auth=self._auth,
                )
        except httpx.TimeoutException as exc:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Timed out while creating the Fuseki dataset",
            ) from exc

        if response.status_code >= 400:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to create Fuseki dataset: {response.text}",
            )

    async def delete_dataset(self, dataset_name: str, ignore_missing: bool = False) -> None:
        """Deletes a dataset from Fuseki."""
        try:
            async with httpx.AsyncClient(timeout=self._admin_timeout) as client:
                response = await client.delete(
                    f"{self._base_url}/$/datasets/{dataset_name}",
                    auth=self._auth,
                )
        except httpx.TimeoutException as exc:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Timed out while deleting the Fuseki dataset",
            ) from exc

        if ignore_missing and response.status_code == 404:
            return

        if response.status_code >= 400:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to delete Fuseki dataset ({response.status_code}): {response.text}",
            )

    async def upload_rdf(self, payload: FusekiUploadPayload) -> None:
        """Uploads ontology RDF content into a Fuseki dataset.

        The upload uses the same file-style request shape that succeeds in the
        Fuseki UI for large ontology files.
        """
        try:
            async with httpx.AsyncClient(timeout=self._upload_timeout) as client:
                response = await client.post(
                    f"{self._base_url}/{payload.dataset_name}/data",
                    files={
                        "file": (
                            payload.filename,
                            payload.content,
                            "application/octet-stream",
                        )
                    },
                    auth=self._auth,
                )
        except httpx.TimeoutException as exc:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Timed out while uploading ontology RDF to Fuseki",
            ) from exc

        if response.status_code >= 400:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to upload RDF to Fuseki: {response.text}",
            )

    async def execute_query(self, dataset_name: str, query: str) -> dict[str, object]:
        """Execute a SPARQL query against a Fuseki dataset and return the JSON response."""
        try:
            async with httpx.AsyncClient(timeout=self._upload_timeout) as client:
                response = await client.post(
                    f"{self.dataset_endpoint(dataset_name)}/query",
                    data={"query": query},
                    headers={"Accept": "application/sparql-results+json, application/json"},
                    auth=self._auth,
                )
        except httpx.TimeoutException as exc:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Timed out while executing the Fuseki query",
            ) from exc

        if response.status_code >= 400:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to execute Fuseki query ({response.status_code}): {response.text}",
            )

        try:
            return response.json()
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Fuseki returned a non-JSON response to the query",
            ) from exc

    async def replace_dataset(
        self,
        dataset_name: str,
        files: list[FusekiUploadPayload],
        previous_dataset_name: str | None,
    ) -> None:
        """Creates the new dataset, uploads all RDF files, then removes the previous dataset."""
        dataset_created = False
        try:
            await self.create_dataset(dataset_name)
            dataset_created = True
            for payload in files:
                await self.upload_rdf(payload)
            if previous_dataset_name and previous_dataset_name != dataset_name:
                await self.delete_dataset(previous_dataset_name, ignore_missing=True)
        except Exception:
            if dataset_created:
                try:
                    await self.delete_dataset(dataset_name, ignore_missing=True)
                except HTTPException:
                    pass
            raise

    def dataset_endpoint(self, dataset_name: str) -> str:
        """Builds the public endpoint URL for a dataset."""
        return f"{self._base_url}/{dataset_name}"
