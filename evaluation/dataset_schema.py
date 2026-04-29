"""Pydantic models for NL-to-SPARQL evaluation datasets and results."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class QueryShape(str, Enum):
    """Structural shape of the gold SPARQL query."""

    SINGLE_EDGE = "single-edge"
    CHAIN = "chain"
    STAR = "star"
    TREE = "tree"
    CYCLE = "cycle"
    FLOWER = "flower"


class ComplexityTier(str, Enum):
    """Broad complexity category."""

    SIMPLE = "simple"
    MEDIUM = "medium"
    COMPLEX = "complex"


class QuestionType(str, Enum):
    """Expected answer format."""

    LIST = "list"
    BOOLEAN = "boolean"
    COUNT = "count"
    FACTOID = "factoid"


class DatasetSource(str, Enum):
    """Origin of the dataset."""

    CUSTOM = "custom"
    QALD = "qald"
    SPIDER4SPARQL = "spider4sparql"
    LCQUAD = "lcquad"


class EvaluationQuestion(BaseModel):
    """A single natural-language question with gold references."""

    id: str = Field(..., description="Unique question identifier, e.g. Q001")
    nl_question: str = Field(..., description="The natural-language question")
    gold_sparql: str = Field(..., description="Gold-standard SPARQL query")
    gold_answers: list[dict[str, str]] = Field(
        default_factory=list,
        description="Expected result rows. Empty means this question is unscored unless the dataset intentionally defines an empty answer set.",
    )
    complexity_tier: ComplexityTier = Field(..., description="Broad complexity category")
    query_shape: QueryShape = Field(..., description="Structural query shape")
    question_type: QuestionType = Field(..., description="Expected answer format")
    notes: Optional[str] = Field(default=None, description="Optional question notes")


class EvaluationDataset(BaseModel):
    """A complete evaluation dataset for one ontology."""

    dataset_name: str = Field(..., description="Unique dataset identifier")
    ontology_file: str = Field(..., description="Ontology filename used by the dataset")
    source: DatasetSource = Field(..., description="Dataset origin")
    description: Optional[str] = Field(default=None, description="Dataset description")
    questions: list[EvaluationQuestion] = Field(..., min_length=1)

    @property
    def size(self) -> int:
        return len(self.questions)

    def by_complexity(self, tier: ComplexityTier) -> list[EvaluationQuestion]:
        return [q for q in self.questions if q.complexity_tier == tier]

    def by_shape(self, shape: QueryShape) -> list[EvaluationQuestion]:
        return [q for q in self.questions if q.query_shape == shape]

    def by_type(self, qtype: QuestionType) -> list[EvaluationQuestion]:
        return [q for q in self.questions if q.question_type == qtype]


class IterationLog(BaseModel):
    """Log of one generation, validation, and execution attempt."""

    iteration: int
    generated_sparql: str
    validation_stages: dict[str, bool] = Field(default_factory=dict)
    validation_errors: list[str] = Field(default_factory=list)
    execution_status: str | None = None


class QuestionResult(BaseModel):
    """Complete result for one evaluated question."""

    question_id: str
    nl_question: str
    gold_sparql: str
    gold_answers: list[dict[str, str]] = Field(default_factory=list)

    final_sparql: Optional[str] = None
    final_answers: Optional[list[dict[str, str]]] = None
    status: str = "pending"
    errors: list[str] = Field(default_factory=list)

    scoring_status: str = "scored"
    is_scored: bool = True
    comparison: dict[str, object] | None = None

    iterations: list[IterationLog] = Field(default_factory=list)
    total_iterations: int = 0

    total_latency_ms: float = 0.0
    total_input_tokens: int = 0
    total_output_tokens: int = 0

    model_name: str = ""
    pipeline_config: dict[str, object] = Field(default_factory=dict)
    trace_path: str | None = None
    readable_trace_path: str | None = None


class ExperimentRun(BaseModel):
    """A full evaluation run against one package and one dataset."""

    experiment_id: str
    dataset_name: str
    package_dir: str
    model_name: str
    pipeline_config: dict[str, object] = Field(default_factory=dict)
    results: list[QuestionResult] = Field(default_factory=list)
    timestamp: str = ""
