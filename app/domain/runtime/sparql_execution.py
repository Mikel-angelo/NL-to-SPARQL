"""Execute SPARQL queries and format execution trace stages.

This module is the only runtime module that talks to the SPARQL endpoint. It
posts a query to the configured endpoint and returns the endpoint JSON response.
It also formats execution success/failure as the same stage-result shape used by
formal validation so the pipeline trace can record execution uniformly.
"""

from __future__ import annotations

import httpx

from app.core.config import settings
from app.domain.runtime.validation import ValidationStageResult


async def execute_sparql_query(endpoint: str, query: str) -> dict[str, object]:
    """Execute one SPARQL query against an explicit endpoint URL.

    The endpoint must be the query endpoint, not just the dataset base URL. HTTP
    status failures are propagated via `httpx` exceptions so the pipeline can
    record them as `EXECUTION_ERROR` and decide whether to request correction.
    """
    if not endpoint:
        raise ValueError("No SPARQL query endpoint is configured")

    async with httpx.AsyncClient(timeout=settings.fuseki_upload_timeout_seconds) as client:
        response = await client.post(
            endpoint,
            data={"query": query},
            headers={"Accept": "application/sparql-results+json, application/json"},
        )
        response.raise_for_status()
    return response.json()


def execution_stage_result(error: Exception | None = None) -> ValidationStageResult:
    """Return the trace-stage result for endpoint execution.

    Execution is not part of formal SPARQL validation, but the trace stores it
    beside validation stages. This helper keeps the execution stage codes stable:
    `EXECUTION_OK` for success and `EXECUTION_ERROR` for endpoint failures.
    """
    if error is None:
        return ValidationStageResult(stage="execution", passed=True, code="EXECUTION_OK")
    return ValidationStageResult(
        stage="execution",
        passed=False,
        code="EXECUTION_ERROR",
        message=f"SPARQL endpoint execution failed: {error}",
    )
