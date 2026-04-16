from typing import Any

from pydantic import BaseModel, Field
from fastapi import APIRouter

from app.services.runtime.query_pipeline import QueryPipelineService


router = APIRouter(prefix="/query", tags=["query"])

query_pipeline_service = QueryPipelineService()


class QueryRequest(BaseModel):
    """Request body for the runtime NL-to-SPARQL route."""

    question: str = Field(min_length=1)


class QueryResponse(BaseModel):
    """Structured runtime response returned by the query pipeline."""

    question: str
    dataset_name: str
    dataset_endpoint: str
    retrieved_context: list[dict[str, Any]]
    generated_sparql: str | None
    validated_sparql: str | None
    corrected_sparql: str | None
    execution_result: dict[str, Any] | None
    status: str
    errors: list[str] | None


@router.post("", response_model=QueryResponse)
async def run_query(request: QueryRequest) -> dict[str, object]:
    """Run the runtime query pipeline for one natural-language question."""
    result = await query_pipeline_service.run(question=request.question)
    return result.to_dict()
