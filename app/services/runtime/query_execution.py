"""Execute validated SPARQL against the active Fuseki dataset."""

from app.services.fuseki import FusekiService


class QueryExecutionService:
    """Small execution wrapper around the Fuseki service."""

    def __init__(self, fuseki_service: FusekiService | None = None) -> None:
        self._fuseki_service = fuseki_service or FusekiService()

    async def execute(self, dataset_name: str, query: str) -> dict[str, object]:
        """Execute the provided SPARQL query and return the Fuseki JSON response."""
        return await self._fuseki_service.execute_query(dataset_name=dataset_name, query=query)
