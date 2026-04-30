"""Direct package evaluator for NL-to-SPARQL experiments."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from app.core.config import settings
from app.domain.ontology.package_activation import resolve_package_reference
from app.domain.package import (
    DomainError,
    get_active_package,
    ontology_context_path,
    read_json_file,
    resolve_package_dir,
    settings_path,
)
from app.domain.rag import SUPPORTED_CHUNKING_ORDER
from app.domain.runtime import run_query_pipeline

from .answer_comparison import ComparisonResult, compare_results
from .dataset_schema import EvaluationDataset, ExperimentRun, IterationLog, QuestionResult
from .metrics import AggregatedMetrics, aggregate_metrics, compute_question_metrics, format_metrics_report


@dataclass(frozen=True)
class ExperimentConfig:
    """Configuration for one direct package evaluation run."""

    package_dir: Path
    model_name: str = ""
    requested_top_k: int | None = None
    package_top_k: int | None = None
    effective_top_k: int | None = None
    requested_chunking_strategy: str | None = None
    package_default_chunking_strategy: str | None = None
    effective_chunking_strategy: str = "class_based"
    endpoint_timeout_seconds: float = 30.0

    def to_dict(self) -> dict[str, object]:
        return {
            "package_dir": str(self.package_dir),
            "model_name": self.model_name,
            "requested_retrieval_top_k": self.requested_top_k,
            "package_retrieval_top_k": self.package_top_k,
            "effective_retrieval_top_k": self.effective_top_k,
            "requested_chunking_strategy": self.requested_chunking_strategy,
            "package_default_chunking_strategy": self.package_default_chunking_strategy,
            "effective_chunking_strategy": self.effective_chunking_strategy,
            "endpoint_timeout_seconds": self.endpoint_timeout_seconds,
            "runner": "direct_package",
        }


class ExperimentRunner:
    """Run a dataset through the runtime pipeline using an explicit package."""

    def __init__(self, config: ExperimentConfig):
        self.config = config

    async def run_experiment(
        self,
        dataset: EvaluationDataset,
        *,
        prefix_map: dict[str, str] | None = None,
    ) -> tuple[ExperimentRun, AggregatedMetrics]:
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M")
        model_label = self.config.model_name or "package-default"
        experiment_id = f"{dataset.dataset_name}_{model_label}_{timestamp}".replace(":", "-").replace("/", "-")

        experiment = ExperimentRun(
            experiment_id=experiment_id,
            dataset_name=dataset.dataset_name,
            package_dir=str(self.config.package_dir),
            model_name=model_label,
            pipeline_config=self.config.to_dict(),
            timestamp=timestamp,
        )

        metrics = []
        total = len(dataset.questions)
        for index, question in enumerate(dataset.questions, 1):
            print(f"[{index}/{total}] {question.id}: {question.nl_question[:80]}")
            question_result, comparison = await self._run_single_question(question, prefix_map)
            experiment.results.append(question_result)
            metrics.append(
                compute_question_metrics(
                    question_result,
                    comparison,
                    complexity_tier=question.complexity_tier.value,
                    query_shape=question.query_shape.value,
                )
            )

            if question_result.is_scored and comparison is not None:
                print(
                    f"  scored exact={comparison.exact_match} "
                    f"f1={comparison.f1:.3f} latency={question_result.total_latency_ms:.0f}ms"
                )
            else:
                print(f"  unscored latency={question_result.total_latency_ms:.0f}ms")

        aggregate = aggregate_metrics(
            metrics,
            dataset_name=dataset.dataset_name,
            model_name=model_label,
        )
        return experiment, aggregate

    async def _run_single_question(
        self,
        question,
        prefix_map: dict[str, str] | None,
    ) -> tuple[QuestionResult, ComparisonResult | None]:
        is_scored = bool(question.gold_answers)
        result = QuestionResult(
            question_id=question.id,
            nl_question=question.nl_question,
            gold_sparql=question.gold_sparql,
            gold_answers=question.gold_answers,
            scoring_status="scored" if is_scored else "missing_gold",
            is_scored=is_scored,
            model_name=self.config.model_name,
            pipeline_config=self.config.to_dict(),
        )

        started = time.perf_counter()
        try:
            pipeline_result = await run_query_pipeline(
                question.nl_question,
                self.config.package_dir,
                model=self.config.model_name or None,
                k=self.config.effective_top_k,
                chunking=self.config.effective_chunking_strategy,
            )
            result.total_latency_ms = (time.perf_counter() - started) * 1000
            result.status = pipeline_result.status
            result.errors = pipeline_result.errors or []
            result.final_sparql = (
                pipeline_result.validated_sparql
                or pipeline_result.corrected_sparql
                or pipeline_result.generated_sparql
            )
            result.final_answers = extract_answers_from_sparql_json(pipeline_result.execution_result)
            result.trace_path = pipeline_result.trace_path
            result.readable_trace_path = pipeline_result.readable_trace_path
            result.iterations = _iteration_logs_from_trace(pipeline_result.trace_path)
            result.total_iterations = len(result.iterations)
        except Exception as exc:
            result.status = "error"
            result.errors = [str(exc)]
            result.total_latency_ms = (time.perf_counter() - started) * 1000

        if not result.is_scored:
            return result, None

        comparison = compare_results(result.final_answers, question.gold_answers, prefix_map=prefix_map)
        result.comparison = asdict(comparison)
        return result, comparison


def extract_answers_from_sparql_json(execution_result: dict[str, object] | None) -> list[dict[str, str]] | None:
    """Flatten SPARQL JSON results into comparison rows."""
    if execution_result is None:
        return None
    if "boolean" in execution_result:
        return [{"result": str(bool(execution_result["boolean"])).lower()}]

    results = execution_result.get("results")
    if not isinstance(results, dict):
        return None
    bindings = results.get("bindings")
    if not isinstance(bindings, list):
        return None

    rows: list[dict[str, str]] = []
    for binding in bindings:
        if not isinstance(binding, dict):
            continue
        row = {}
        for variable, value in binding.items():
            if isinstance(value, dict) and "value" in value:
                row[str(variable)] = str(value["value"])
        rows.append(row)
    return rows


async def preflight_endpoint(endpoint: str, *, timeout: float) -> None:
    """Check the configured SPARQL endpoint once before timed evaluation starts."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            endpoint,
            data={"query": "ASK WHERE { ?s ?p ?o }"},
            headers={"Accept": "application/sparql-results+json, application/json"},
        )
        response.raise_for_status()


def ensure_requested_package_is_active(package_dir: str | Path, packages_root: str | Path) -> Path:
    """Return the resolved package path if it is the active package."""
    requested = resolve_package_reference(package_dir, packages_root)
    active = get_active_package(packages_root).resolve()
    if requested.resolve() != active:
        raise DomainError(
            f"Requested package is not active. Requested={requested}; active={active}. "
            "Run activate.py first."
        )
    return requested


def save_experiment(
    experiment: ExperimentRun,
    metrics: AggregatedMetrics,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Save raw results, aggregate metrics, and readable evaluation logs."""
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    run_file = output / "results.json"
    metrics_file = output / "metrics.json"
    report_file = output / "report.txt"
    config_file = output / "run_config.json"

    run_file.write_text(_model_json(experiment), encoding="utf-8")
    metrics_file.write_text(json.dumps(metrics.to_dict(), indent=2), encoding="utf-8")
    config_file.write_text(json.dumps(_run_config_payload(experiment), indent=2), encoding="utf-8")
    report_file.write_text(format_metrics_report(metrics), encoding="utf-8")
    readable_files = write_evaluation_query_logs(output, experiment)
    return {
        "results": run_file,
        "metrics": metrics_file,
        "run_config": config_file,
        "report": report_file,
        **readable_files,
    }


def write_evaluation_query_logs(output_dir: Path, experiment: ExperimentRun) -> dict[str, Path]:
    """Write compact per-question logs owned by one evaluation run."""
    queries_dir = output_dir / "queries"
    queries_dir.mkdir(parents=True, exist_ok=True)
    index_file = output_dir / "index.txt"
    jsonl_file = output_dir / "queries.jsonl"

    index_lines = [
        f"Evaluation: {experiment.experiment_id}",
        f"Dataset: {experiment.dataset_name}",
        f"Package: {experiment.package_dir}",
        f"Chunking: {experiment.pipeline_config.get('effective_chunking_strategy')}",
        f"Retrieval top-k: {experiment.pipeline_config.get('effective_retrieval_top_k')}",
        "",
    ]
    with jsonl_file.open("w", encoding="utf-8") as jsonl:
        for result in experiment.results:
            jsonl.write(json.dumps(_model_dict(result), ensure_ascii=False) + "\n")
            (queries_dir / f"{_safe_filename(result.question_id)}.txt").write_text(
                format_question_log(result),
                encoding="utf-8",
            )
            index_lines.append(format_index_line(result))

    index_file.write_text("\n".join(index_lines) + "\n", encoding="utf-8")
    return {"index": index_file, "queries_jsonl": jsonl_file, "queries_dir": queries_dir}


def format_index_line(result: QuestionResult) -> str:
    """Return one scan-friendly row for the evaluation index."""
    if not result.is_scored:
        marker = "UNSCORED"
        f1 = ""
    else:
        exact = bool((result.comparison or {}).get("exact_match"))
        marker = "PASS" if exact else "FAIL"
        f1_value = (result.comparison or {}).get("f1")
        f1 = f" f1={float(f1_value):.3f}" if isinstance(f1_value, (int, float)) else ""
    return (
        f"{result.question_id:>5} {marker:<8} {result.status:<9}"
        f"{f1:<10} {result.nl_question}"
    )


def format_question_log(result: QuestionResult) -> str:
    """Format one evaluated question as plain text for debugging."""
    comparison = result.comparison or {}
    lines = [
        f"QUESTION {result.question_id}",
        result.nl_question,
        "",
        "SCORING",
        f"Status: {result.scoring_status}",
        f"Pipeline status: {result.status}",
    ]
    if result.is_scored:
        lines.extend(
            [
                f"Exact match: {_value(comparison.get('exact_match'))}",
                f"Precision: {_float(comparison.get('precision'))}",
                f"Recall: {_float(comparison.get('recall'))}",
                f"F1: {_float(comparison.get('f1'))}",
            ]
        )
    lines.extend(
        [
            "",
            "GOLD SPARQL",
            result.gold_sparql or "",
            "",
            "FINAL SPARQL",
            result.final_sparql or "",
            "",
            "GOLD ANSWERS",
            _format_rows(result.gold_answers),
            "",
            "GENERATED ANSWERS",
            _format_rows(result.final_answers),
        ]
    )
    if result.is_scored:
        lines.extend(
            [
                "",
                "DIFF",
                "Missing:",
                _format_tuple_rows(comparison.get("missing_rows")),
                "Extra:",
                _format_tuple_rows(comparison.get("extra_rows")),
            ]
        )
    lines.extend(
        [
            "",
            "PIPELINE",
            f"Iterations: {result.total_iterations}",
            f"Latency ms: {result.total_latency_ms:.0f}",
            f"Requested retrieval top-k: {result.pipeline_config.get('requested_retrieval_top_k')}",
            f"Package retrieval top-k: {result.pipeline_config.get('package_retrieval_top_k')}",
            f"Effective retrieval top-k: {result.pipeline_config.get('effective_retrieval_top_k')}",
            f"Requested chunking strategy: {result.pipeline_config.get('requested_chunking_strategy')}",
            f"Package default chunking strategy: {result.pipeline_config.get('package_default_chunking_strategy')}",
            f"Effective chunking strategy: {result.pipeline_config.get('effective_chunking_strategy')}",
            f"Trace: {result.trace_path or ''}",
            f"Readable trace: {result.readable_trace_path or ''}",
        ]
    )
    if result.errors:
        lines.extend(["", "ERRORS", *[f"- {error}" for error in result.errors]])
    return "\n".join(lines) + "\n"


def load_dataset(path: str | Path) -> EvaluationDataset:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return EvaluationDataset(**payload)


def prefix_map_from_package(package_dir: str | Path) -> dict[str, str]:
    context = read_json_file(ontology_context_path(package_dir))
    prefixes = context.get("prefixes")
    prefix_map = {}
    if isinstance(prefixes, list):
        for item in prefixes:
            if not isinstance(item, dict):
                continue
            prefix = item.get("prefix")
            namespace = item.get("namespace")
            if isinstance(prefix, str) and isinstance(namespace, str):
                prefix_map[prefix] = namespace
    return prefix_map


def _int_setting(payload: dict[str, object], key: str) -> int | None:
    value = payload.get(key)
    return int(value) if isinstance(value, (int, float)) else None


def _string_setting(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) and value.strip() else None


def default_output_dir(package_dir: str | Path, dataset_name: str) -> Path:
    root = resolve_package_dir(package_dir) / "evaluation"
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M")
    stem = f"{dataset_name}-{timestamp}"
    candidate = root / stem
    index = 2
    while candidate.exists():
        candidate = root / f"{stem}-{index}"
        index += 1
    return candidate


async def run_from_cli(args: argparse.Namespace) -> dict[str, Path]:
    package_dir = ensure_requested_package_is_active(args.package, settings.ontology_packages_path)
    package_settings = read_json_file(settings_path(package_dir))
    endpoint = package_settings.get("query_endpoint")
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise DomainError(f"Active package has no query_endpoint: {package_dir}")

    print(f"Preflight endpoint: {endpoint}")
    await preflight_endpoint(endpoint, timeout=args.preflight_timeout)

    dataset = load_dataset(args.dataset)
    output_dir = Path(args.output) if args.output else default_output_dir(package_dir, dataset.dataset_name)
    package_default_chunking = _string_setting(package_settings, "default_chunking_strategy")
    config = ExperimentConfig(
        package_dir=package_dir,
        model_name=args.model or "",
        requested_top_k=args.k,
        package_top_k=_int_setting(package_settings, "retrieval_top_k"),
        effective_top_k=args.k or _int_setting(package_settings, "retrieval_top_k") or settings.runtime_retrieval_top_k,
        requested_chunking_strategy=args.chunking,
        package_default_chunking_strategy=package_default_chunking,
        effective_chunking_strategy=args.chunking or package_default_chunking or "class_based",
        endpoint_timeout_seconds=args.preflight_timeout,
    )
    runner = ExperimentRunner(config)
    experiment, metrics = await runner.run_experiment(dataset, prefix_map=prefix_map_from_package(package_dir))
    saved = save_experiment(experiment, metrics, output_dir)

    print()
    print(format_metrics_report(metrics))
    print()
    print(f"Results: {saved['results']}")
    print(f"Metrics: {saved['metrics']}")
    print(f"Run config: {saved['run_config']}")
    print(f"Report:  {saved['report']}")
    return saved


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate a prepared ontology package.")
    parser.add_argument("--dataset", required=True, help="Evaluation dataset JSON path")
    parser.add_argument("--package", required=True, help="Active package directory path or name")
    parser.add_argument("--model", default="", help="Optional model override")
    parser.add_argument("--k", type=int, default=None, help="Optional retrieval top-k override")
    parser.add_argument("--chunking", choices=SUPPORTED_CHUNKING_ORDER, default=None, help="Optional retrieval index strategy override")
    parser.add_argument("--output", default="", help="Optional output directory. Defaults to <package>/evaluation/<run-id>/")
    parser.add_argument("--preflight-timeout", type=float, default=30.0, help="Endpoint preflight timeout in seconds")
    return parser.parse_args()


def _iteration_logs_from_trace(trace_path: str) -> list[IterationLog]:
    path = Path(trace_path)
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    latest = payload[-1] if isinstance(payload, list) and payload else {}
    attempts = latest.get("correction_iterations") if isinstance(latest, dict) else None
    if not isinstance(attempts, list):
        return []

    logs: list[IterationLog] = []
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        validation = attempt.get("validation")
        stages = {}
        errors = []
        if isinstance(validation, dict):
            errors = [str(error) for error in validation.get("errors", []) if isinstance(error, str)]
            raw_stages = validation.get("stages")
            if isinstance(raw_stages, list):
                for stage in raw_stages:
                    if isinstance(stage, dict) and isinstance(stage.get("stage"), str):
                        stages[str(stage["stage"])] = bool(stage.get("passed"))
        execution = attempt.get("execution")
        execution_status = None
        if isinstance(execution, dict):
            execution_status = str(execution.get("code") or "")
        logs.append(
            IterationLog(
                iteration=int(attempt.get("iteration", len(logs) + 1)),
                generated_sparql=str(attempt.get("query") or ""),
                validation_stages=stages,
                validation_errors=errors,
                execution_status=execution_status,
            )
        )
    return logs


def _model_json(model) -> str:
    if hasattr(model, "model_dump_json"):
        return model.model_dump_json(indent=2)
    return model.json(indent=2)


def _model_dict(model) -> dict[str, object]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return json.loads(model.json())


def _run_config_payload(experiment: ExperimentRun) -> dict[str, object]:
    return {
        "experiment_id": experiment.experiment_id,
        "dataset_name": experiment.dataset_name,
        "package_dir": experiment.package_dir,
        "model_name": experiment.model_name,
        "timestamp": experiment.timestamp,
        "pipeline_config": experiment.pipeline_config,
    }


def _safe_filename(value: str) -> str:
    return "".join(char if char.isalnum() or char in ("-", "_") else "_" for char in value) or "question"


def _format_rows(rows: list[dict[str, str]] | None) -> str:
    if rows is None:
        return "- None"
    if not rows:
        return "- []"
    return "\n".join("- " + ", ".join(f"{key}={value}" for key, value in row.items()) for row in rows)


def _format_tuple_rows(rows: object) -> str:
    if not isinstance(rows, list) or not rows:
        return "- None"
    rendered = []
    for row in rows:
        if isinstance(row, (list, tuple)):
            rendered.append("- " + ", ".join(str(value) for value in row))
        else:
            rendered.append(f"- {row}")
    return "\n".join(rendered)


def _float(value: object) -> str:
    return f"{float(value):.3f}" if isinstance(value, (int, float)) else ""


def _value(value: object) -> str:
    return "" if value is None else str(value)


async def main() -> None:
    try:
        await run_from_cli(parse_args())
    except (DomainError, httpx.HTTPError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    asyncio.run(main())
