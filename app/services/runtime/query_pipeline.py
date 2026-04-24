"""Orchestrate the runtime NL-to-SPARQL pipeline."""

from dataclasses import dataclass
from pathlib import Path
import json
from datetime import UTC, datetime

from fastapi import HTTPException, status

from app.core.config import settings
from app.services.fuseki import FusekiService
from app.services.runtime.query_correction import QueryCorrectionService
from app.services.runtime.query_execution import QueryExecutionService
from app.services.runtime.query_generation import LLMClient, PromptBuilder, normalize_generated_query
from app.services.runtime.query_validation import QueryValidationService
from app.services.runtime.rag_retrieval_service import RAGRetrievalService


@dataclass(frozen=True)
class QueryPipelineResult:
    """Structured response returned by the runtime query pipeline."""

    question: str
    dataset_name: str
    dataset_endpoint: str
    retrieved_context: list[dict[str, object]]
    generated_sparql: str | None
    validated_sparql: str | None
    corrected_sparql: str | None
    execution_result: dict[str, object] | None
    status: str
    errors: list[str] | None

    def to_dict(self) -> dict[str, object]:
        return {
            "question": self.question,
            "dataset_name": self.dataset_name,
            "dataset_endpoint": self.dataset_endpoint,
            "retrieved_context": self.retrieved_context,
            "generated_sparql": self.generated_sparql,
            "validated_sparql": self.validated_sparql,
            "corrected_sparql": self.corrected_sparql,
            "execution_result": self.execution_result,
            "status": self.status,
            "errors": self.errors,
        }


class QueryPipelineService:
    """Run retrieval, generation, validation, correction, and execution."""

    def __init__(
        self,
        storage_dir: Path | None = None,
        fuseki_service: FusekiService | None = None,
        rag_retrieval_service: RAGRetrievalService | None = None,
        prompt_builder: PromptBuilder | None = None,
        llm_client: LLMClient | None = None,
        query_validation_service: QueryValidationService | None = None,
        query_correction_service: QueryCorrectionService | None = None,
        query_execution_service: QueryExecutionService | None = None,
    ) -> None:
        self._storage_dir = storage_dir or Path(settings.storage_path)
        self._current_dir = self._storage_dir / "current"
        self._metadata_path = self._current_dir / "metadata.json"
        self._ontology_context_path = self._current_dir / "ontology_context.json"
        self._query_pipeline_log_path = self._current_dir / "query_pipeline_log.json"
        self._fuseki_service = fuseki_service or FusekiService()
        self._rag_retrieval_service = rag_retrieval_service or RAGRetrievalService(
            storage_dir=self._storage_dir
        )
        self._prompt_builder = prompt_builder or PromptBuilder(
            storage_dir=self._storage_dir
        )
        self._llm_client = llm_client or LLMClient()
        self._query_validation_service = query_validation_service or QueryValidationService()
        self._query_correction_service = query_correction_service or QueryCorrectionService()
        self._query_execution_service = query_execution_service or QueryExecutionService(
            fuseki_service=self._fuseki_service
        )

    async def run(self, question: str) -> QueryPipelineResult:
        """Run the full runtime query flow and return a structured trace."""
        metadata = self._load_current_metadata()
        ontology_context = self._load_current_ontology_context()
        dataset_name = self._dataset_name_from_metadata(metadata)
        dataset_endpoint = self._fuseki_service.dataset_endpoint(dataset_name)
        pipeline_log = self._new_pipeline_log(
            question=question,
            dataset_name=dataset_name,
            dataset_endpoint=dataset_endpoint,
        )

        try:
            retrieved_context = self._rag_retrieval_service.retrieve(question)
            pipeline_log["chunks_retrieved"] = retrieved_context

            prompt = self._prompt_builder.render_prompt(
                question=question,
                retrieved_context=retrieved_context,
                metadata=metadata,
            )
            pipeline_log["prompt_generated"] = prompt

            generated_sparql = normalize_generated_query(
                await self._llm_client.generate_text(prompt)
            )
            pipeline_log["llm_generated_query"] = generated_sparql

            corrected_sparql = None
            validation_result = self._query_validation_service.validate(
                generated_sparql,
                ontology_context=ontology_context,
            )
            pipeline_log["validation"] = {
                "is_valid": validation_result.is_valid,
                "errors": validation_result.errors,
                "normalized_query": validation_result.normalized_query,
            }

            final_query = validation_result.normalized_query
            final_validation_result = validation_result

            if not validation_result.is_valid:
                corrected_sparql = self._query_correction_service.correct(
                    query=generated_sparql,
                    errors=validation_result.errors,
                )
                pipeline_log["returned_corrected_query"] = corrected_sparql
                pipeline_log["llm_new_query"] = None

                if corrected_sparql:
                    corrected_validation_result = self._query_validation_service.validate(
                        corrected_sparql,
                        ontology_context=ontology_context,
                    )
                    pipeline_log["corrected_validation"] = {
                        "is_valid": corrected_validation_result.is_valid,
                        "errors": corrected_validation_result.errors,
                        "normalized_query": corrected_validation_result.normalized_query,
                    }
                    final_query = corrected_validation_result.normalized_query
                    final_validation_result = corrected_validation_result

            if final_validation_result.is_valid:
                pipeline_log["execution_query_to_fuseki"] = final_query
                execution_result = await self._query_execution_service.execute(
                    dataset_name=dataset_name,
                    query=final_query,
                )
                pipeline_log["fuseki_response"] = execution_result
                result = QueryPipelineResult(
                    question=question,
                    dataset_name=dataset_name,
                    dataset_endpoint=dataset_endpoint,
                    retrieved_context=retrieved_context,
                    generated_sparql=generated_sparql,
                    validated_sparql=final_query,
                    corrected_sparql=corrected_sparql,
                    execution_result=execution_result,
                    status="completed",
                    errors=None,
                )
            else:
                result = QueryPipelineResult(
                    question=question,
                    dataset_name=dataset_name,
                    dataset_endpoint=dataset_endpoint,
                    retrieved_context=retrieved_context,
                    generated_sparql=generated_sparql,
                    validated_sparql=None,
                    corrected_sparql=corrected_sparql,
                    execution_result=None,
                    status="failed",
                    errors=final_validation_result.errors,
                )

            self._finalize_pipeline_log(pipeline_log, result)
            return result
        except Exception as exc:
            pipeline_log["pipeline_exception"] = str(exc)
            self._write_pipeline_log(pipeline_log)
            raise

    def _load_current_metadata(self) -> dict[str, object]:
        if not self._metadata_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No active ontology dataset is available. Load an ontology first.",
            )

        try:
            metadata = json.loads(self._metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to load current runtime metadata",
            ) from exc

        if not isinstance(metadata, dict):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Current runtime metadata has an invalid format",
            )
        return metadata

    def _load_current_ontology_context(self) -> dict[str, object]:
        if not self._ontology_context_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="ontology_context.json not found for the active ontology",
            )

        try:
            ontology_context = json.loads(self._ontology_context_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to load current ontology context",
            ) from exc

        if not isinstance(ontology_context, dict):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Current ontology context has an invalid format",
            )
        return ontology_context

    @staticmethod
    def _dataset_name_from_metadata(metadata: dict[str, object]) -> str:
        dataset_name = metadata.get("dataset_name")
        if not isinstance(dataset_name, str) or not dataset_name.strip():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="The current runtime metadata is missing dataset_name",
            )
        return dataset_name.strip()

    def _new_pipeline_log(
        self,
        question: str,
        dataset_name: str,
        dataset_endpoint: str,
    ) -> dict[str, object]:
        return {
            "run_at": datetime.now(UTC).isoformat(),
            "question_asked": question,
            "dataset_name": dataset_name,
            "dataset_endpoint": dataset_endpoint,
            "chunks_retrieved": None,
            "prompt_generated": None,
            "llm_generated_query": None,
            "validation": None,
            "returned_corrected_query": None,
            "llm_new_query": None,
            "corrected_validation": None,
            "execution_query_to_fuseki": None,
            "fuseki_response": None,
            "result_status": None,
            "result_errors": None,
            "pipeline_exception": None,
        }

    def _finalize_pipeline_log(
        self,
        pipeline_log: dict[str, object],
        result: QueryPipelineResult,
    ) -> None:
        pipeline_log["result_status"] = result.status
        pipeline_log["result_errors"] = result.errors
        self._write_pipeline_log(pipeline_log)

    def _write_pipeline_log(self, pipeline_log: dict[str, object]) -> None:
        self._query_pipeline_log_path.parent.mkdir(parents=True, exist_ok=True)
        existing_log = self._read_existing_pipeline_log()
        existing_log.append(pipeline_log)
        self._query_pipeline_log_path.write_text(
            json.dumps(existing_log, indent=2),
            encoding="utf-8",
        )

    def _read_existing_pipeline_log(self) -> list[dict[str, object]]:
        if not self._query_pipeline_log_path.exists():
            return []

        try:
            payload = json.loads(self._query_pipeline_log_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []

        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            return [payload]
        return []
