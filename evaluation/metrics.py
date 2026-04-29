"""Aggregate evaluation results into correctness and runtime metrics."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Optional

from .answer_comparison import ComparisonResult
from .dataset_schema import ComplexityTier, QueryShape, QuestionResult


@dataclass
class QuestionMetrics:
    """Metrics for a single evaluated question."""

    question_id: str
    is_scored: bool = True
    scoring_status: str = "scored"

    exact_match: bool = False
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0

    syntactically_valid_initial: bool = False
    syntactically_valid_final: bool = False
    execution_success: bool = False
    status: str = "pending"

    total_iterations: int = 0
    was_corrected: bool = False
    correction_succeeded: bool = False

    total_latency_ms: float = 0.0
    total_input_tokens: int = 0
    total_output_tokens: int = 0

    complexity_tier: Optional[str] = None
    query_shape: Optional[str] = None


@dataclass
class AggregatedMetrics:
    """Dataset-level metrics for one run."""

    dataset_name: str = ""
    model_name: str = ""
    num_questions: int = 0
    num_scored: int = 0
    num_unscored: int = 0

    execution_accuracy: float = 0.0
    macro_precision: float = 0.0
    macro_recall: float = 0.0
    macro_f1: float = 0.0

    syntactic_validity_rate_initial: float = 0.0
    syntactic_validity_rate_final: float = 0.0
    execution_success_rate: float = 0.0

    correction_improvement_rate: float = 0.0
    avg_iterations: float = 0.0

    avg_latency_ms: float = 0.0
    avg_input_tokens: float = 0.0
    avg_output_tokens: float = 0.0

    ea_by_complexity: dict[str, float] = field(default_factory=dict)
    f1_by_complexity: dict[str, float] = field(default_factory=dict)
    ea_by_shape: dict[str, float] = field(default_factory=dict)
    f1_by_shape: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def compute_question_metrics(
    question_result: QuestionResult,
    comparison: ComparisonResult | None,
    complexity_tier: Optional[str] = None,
    query_shape: Optional[str] = None,
) -> QuestionMetrics:
    """Compute metrics for one question. Correctness is omitted for unscored questions."""
    qm = QuestionMetrics(
        question_id=question_result.question_id,
        is_scored=question_result.is_scored,
        scoring_status=question_result.scoring_status,
        status=question_result.status,
        total_iterations=question_result.total_iterations,
        total_latency_ms=question_result.total_latency_ms,
        total_input_tokens=question_result.total_input_tokens,
        total_output_tokens=question_result.total_output_tokens,
        complexity_tier=complexity_tier,
        query_shape=query_shape,
    )

    if comparison is not None:
        qm.exact_match = comparison.exact_match
        qm.precision = comparison.precision
        qm.recall = comparison.recall
        qm.f1 = comparison.f1

    qm.execution_success = question_result.status == "completed" and question_result.final_answers is not None

    if question_result.iterations:
        first_iter = question_result.iterations[0]
        last_iter = question_result.iterations[-1]
        qm.syntactically_valid_initial = first_iter.validation_stages.get("syntactic", False) or first_iter.validation_stages.get("syntax", False)
        qm.syntactically_valid_final = last_iter.validation_stages.get("syntactic", False) or last_iter.validation_stages.get("syntax", False)

    qm.was_corrected = question_result.total_iterations > 1
    qm.correction_succeeded = qm.was_corrected and qm.exact_match
    return qm


def aggregate_metrics(
    question_metrics: list[QuestionMetrics],
    *,
    dataset_name: str = "",
    model_name: str = "",
) -> AggregatedMetrics:
    """Aggregate per-question metrics. Correctness denominators use scored questions only."""
    if not question_metrics:
        return AggregatedMetrics(dataset_name=dataset_name, model_name=model_name)

    all_count = len(question_metrics)
    scored = [qm for qm in question_metrics if qm.is_scored]
    scored_count = len(scored)

    agg = AggregatedMetrics(
        dataset_name=dataset_name,
        model_name=model_name,
        num_questions=all_count,
        num_scored=scored_count,
        num_unscored=all_count - scored_count,
    )

    agg.execution_accuracy = _rate(sum(1 for qm in scored if qm.exact_match), scored_count)
    agg.macro_precision = _mean([qm.precision for qm in scored])
    agg.macro_recall = _mean([qm.recall for qm in scored])
    agg.macro_f1 = _mean([qm.f1 for qm in scored])

    agg.syntactic_validity_rate_initial = _rate(
        sum(1 for qm in question_metrics if qm.syntactically_valid_initial),
        all_count,
    )
    agg.syntactic_validity_rate_final = _rate(
        sum(1 for qm in question_metrics if qm.syntactically_valid_final),
        all_count,
    )
    agg.execution_success_rate = _rate(
        sum(1 for qm in question_metrics if qm.execution_success),
        all_count,
    )

    corrected = [qm for qm in scored if qm.was_corrected]
    agg.correction_improvement_rate = _rate(
        sum(1 for qm in corrected if qm.correction_succeeded),
        len(corrected),
    )
    agg.avg_iterations = _mean([float(qm.total_iterations) for qm in question_metrics])
    agg.avg_latency_ms = _mean([qm.total_latency_ms for qm in question_metrics])
    agg.avg_input_tokens = _mean([float(qm.total_input_tokens) for qm in question_metrics])
    agg.avg_output_tokens = _mean([float(qm.total_output_tokens) for qm in question_metrics])

    for tier in ComplexityTier:
        tier_scored = [qm for qm in scored if qm.complexity_tier == tier.value]
        if tier_scored:
            agg.ea_by_complexity[tier.value] = _rate(sum(1 for qm in tier_scored if qm.exact_match), len(tier_scored))
            agg.f1_by_complexity[tier.value] = _mean([qm.f1 for qm in tier_scored])

    for shape in QueryShape:
        shape_scored = [qm for qm in scored if qm.query_shape == shape.value]
        if shape_scored:
            agg.ea_by_shape[shape.value] = _rate(sum(1 for qm in shape_scored if qm.exact_match), len(shape_scored))
            agg.f1_by_shape[shape.value] = _mean([qm.f1 for qm in shape_scored])

    return agg


def format_metrics_report(agg: AggregatedMetrics) -> str:
    """Format aggregated metrics as plain ASCII text."""
    lines = [
        "=" * 60,
        f"Evaluation Report: {agg.dataset_name} x {agg.model_name}",
        "=" * 60,
        f"Questions run:       {agg.num_questions}",
        f"Scored questions:    {agg.num_scored}",
        f"Unscored questions:  {agg.num_unscored}",
        "",
        "Answer Correctness",
        f"  Execution Accuracy: {agg.execution_accuracy:.1%}",
        f"  Macro Precision:    {agg.macro_precision:.3f}",
        f"  Macro Recall:       {agg.macro_recall:.3f}",
        f"  Macro F1:           {agg.macro_f1:.3f}",
        "",
        "Pipeline Reliability",
        f"  SVR initial:        {agg.syntactic_validity_rate_initial:.1%}",
        f"  SVR final:          {agg.syntactic_validity_rate_final:.1%}",
        f"  Execution Success:  {agg.execution_success_rate:.1%}",
        "",
        "Self-Correction",
        f"  Correction Rate:    {agg.correction_improvement_rate:.1%}",
        f"  Avg Iterations:     {agg.avg_iterations:.2f}",
        "",
        "Efficiency",
        f"  Avg Latency:        {agg.avg_latency_ms:.0f} ms",
        f"  Avg Input Tokens:   {agg.avg_input_tokens:.0f}",
        f"  Avg Output Tokens:  {agg.avg_output_tokens:.0f}",
    ]

    if agg.ea_by_complexity:
        lines.extend(["", "EA by Complexity"])
        for tier, ea in sorted(agg.ea_by_complexity.items()):
            lines.append(f"  {tier:10s} EA={ea:.1%} F1={agg.f1_by_complexity.get(tier, 0.0):.3f}")

    if agg.ea_by_shape:
        lines.extend(["", "EA by Query Shape"])
        for shape, ea in sorted(agg.ea_by_shape.items()):
            lines.append(f"  {shape:15s} EA={ea:.1%} F1={agg.f1_by_shape.get(shape, 0.0):.3f}")

    lines.append("=" * 60)
    return "\n".join(lines)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _rate(count: int, total: int) -> float:
    return count / total if total else 0.0
