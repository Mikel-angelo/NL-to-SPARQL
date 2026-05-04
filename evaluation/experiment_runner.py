"""Run evaluation datasets against prepared ontology packages.

This module takes an active ontology package plus a dataset JSON file, runs each
natural-language question through the same runtime pipeline used by `query.py`,
compares generated answer rows with the dataset's gold answers, aggregates
metrics, and writes machine-readable plus human-readable artifacts.

The root-level `evaluate.py` owns CLI parsing. The core execution path here is
`ExperimentRunner.run_experiment()`, while `run_from_cli()` handles package
checks, endpoint preflight, dataset loading, configuration defaults, and output
persistence after arguments have already been parsed.
"""

from __future__ import annotations

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
from app.domain.runtime import run_query_pipeline

from .answer_comparison import ComparisonResult, compare_results
from .dataset_schema import EvaluationDataset, ExperimentRun, IterationLog, QuestionResult
from .metrics import AggregatedMetrics, aggregate_metrics, compute_question_metrics, format_metrics_report


@dataclass(frozen=True)
class ExperimentConfig:
    """Runtime configuration for one direct package evaluation run.

    These fields are the controlled variables of an experiment: package,
    optional model override, retrieval top-k, chunking strategy, and correction
    loop limit. The same payload is saved to `run_config.json` so evaluation
    results can be interpreted later.
    """

    package_dir: Path
    model_name: str = ""
    retrieval_top_k: int = 10
    chunking_strategy: str = "class_based"
    correction_max_iterations: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "package_dir": str(self.package_dir),
            "model_name": self.model_name,
            "retrieval_top_k": self.retrieval_top_k,
            "chunking_strategy": self.chunking_strategy,
            "correction_max_iterations": self.correction_max_iterations,
            "runner": "direct_package",
        }


class ExperimentRunner:
    """Execute all questions in one dataset against one package configuration."""

    def __init__(self, config: ExperimentConfig):
        self.config = config

    async def run_experiment(
        self,
        dataset: EvaluationDataset,
        *,
        prefix_map: dict[str, str] | None = None,
    ) -> tuple[ExperimentRun, AggregatedMetrics]:
        """Run every dataset question and return raw results plus aggregate metrics.

        `prefix_map` is used only for answer comparison, allowing prefixed gold
        answers and full-URI generated answers to compare equal.
        """
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
        """Run one question through the runtime pipeline and score it if possible."""
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
                k=self.config.retrieval_top_k,
                chunking=self.config.chunking_strategy,
                corrections=self.config.correction_max_iterations,
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
    """Flatten runtime SPARQL JSON output into rows accepted by `compare_results()`.

    Returns `None` when the pipeline produced no execution result. Boolean ASK
    responses are represented as a single row with a `result` value so they can
    use the same comparison path as SELECT rows.
    """
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
    """Verify the package query endpoint is reachable before timed evaluation.

    This avoids recording many per-question failures when the selected Fuseki
    dataset is not loaded or the endpoint is unavailable.
    """
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            endpoint,
            data={"query": "ASK WHERE { ?s ?p ?o }"},
            headers={"Accept": "application/sparql-results+json, application/json"},
        )
        response.raise_for_status()


def ensure_requested_package_is_active(package_dir: str | Path, packages_root: str | Path) -> Path:
    """Resolve a package reference and require it to match `.active_package`.

    Evaluation does not activate packages automatically. For local file
    packages, activation is what reloads Fuseki, so this check prevents running
    a dataset against stale endpoint contents.
    """
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
    """Persist all artifacts for one evaluation run.

    The output directory receives JSON results, JSON metrics, `run_config.json`,
    a top-level report, an index of question outcomes, compact JSONL question
    records, and one readable text file per question.
    """
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    run_file = output / "results.json"
    metrics_file = output / "metrics.json"
    report_file = output / "report.txt"
    config_file = output / "run_config.json"

    run_file.write_text(_model_json(experiment), encoding="utf-8")
    metrics_file.write_text(json.dumps(metrics.to_dict(), indent=2), encoding="utf-8")
    config_file.write_text(json.dumps(_run_config_payload(experiment), indent=2), encoding="utf-8")
    report_file.write_text(format_experiment_report(experiment, metrics), encoding="utf-8")
    readable_files = write_evaluation_query_logs(output, experiment)
    return {
        "results": run_file,
        "metrics": metrics_file,
        "run_config": config_file,
        "report": report_file,
        **readable_files,
    }


def write_evaluation_query_logs(output_dir: Path, experiment: ExperimentRun) -> dict[str, Path]:
    """Write the question-level readable and JSONL logs for one run."""
    queries_dir = output_dir / "queries"
    queries_dir.mkdir(parents=True, exist_ok=True)
    index_file = output_dir / "index.txt"
    jsonl_file = output_dir / "queries.jsonl"

    index_lines = [
        f"Evaluation: {experiment.experiment_id}",
        f"Dataset: {experiment.dataset_name}",
        f"Package: {experiment.package_dir}",
        f"Chunking: {experiment.pipeline_config.get('chunking_strategy')}",
        f"Retrieval top-k: {experiment.pipeline_config.get('retrieval_top_k')}",
        "",
    ]
    with jsonl_file.open("w", encoding="utf-8") as jsonl:
        for result in experiment.results:
            if not result.pipeline_config:
                result.pipeline_config = dict(experiment.pipeline_config)
            jsonl.write(json.dumps(_model_dict(result), ensure_ascii=False) + "\n")
            (queries_dir / f"{_safe_filename(result.question_id)}.txt").write_text(
                format_question_log(result),
                encoding="utf-8",
            )
            index_lines.append(format_index_line(result))

    index_file.write_text("\n".join(index_lines) + "\n", encoding="utf-8")
    return {"index": index_file, "queries_jsonl": jsonl_file, "queries_dir": queries_dir}


def format_experiment_report(experiment: ExperimentRun, metrics: AggregatedMetrics) -> str:
    """Format the run-level report with configuration followed by metrics."""
    config = experiment.pipeline_config
    header = (
        "Run Configuration\n"
        f"Experiment: {experiment.experiment_id}\n"
        f"Dataset: {experiment.dataset_name}\n"
        f"Package: {experiment.package_dir}\n"
        f"Model: {experiment.model_name}\n"
        f"Retrieval top-k: {config.get('retrieval_top_k')}\n"
        f"Chunking: {config.get('chunking_strategy')}\n"
        f"Correction attempts max: {config.get('correction_max_iterations')}\n"
    )
    return header + "\n" + format_metrics_report(metrics)


def format_index_line(result: QuestionResult) -> str:
    """Return one compact status line for `index.txt`."""
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
    """Format one evaluated question as a standalone debugging report.

    The log includes scoring status, gold and generated SPARQL, gold and
    generated answers, row-level diff information, runtime configuration, and
    links to the detailed runtime traces.
    """
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
            f"Retrieval top-k: {result.pipeline_config.get('retrieval_top_k')}",
            f"Chunking strategy: {result.pipeline_config.get('chunking_strategy')}",
            f"Correction attempts max: {result.pipeline_config.get('correction_max_iterations')}",
            f"Trace: {result.trace_path or ''}",
            f"Readable trace: {result.readable_trace_path or ''}",
        ]
    )
    if result.errors:
        lines.extend(["", "ERRORS", *[f"- {error}" for error in result.errors]])
    return "\n".join(lines) + "\n"


def load_dataset(path: str | Path) -> EvaluationDataset:
    """Load and validate a dataset JSON file using `EvaluationDataset`."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return EvaluationDataset(**payload)


def prefix_map_from_package(package_dir: str | Path) -> dict[str, str]:
    """Extract ontology prefixes from a package for normalized answer comparison."""
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
    """Return a timestamped, non-overwriting output directory under the package."""
    root = resolve_package_dir(package_dir) / "evaluation"
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M")
    stem = f"{dataset_name}-{timestamp}"
    candidate = root / stem
    index = 2
    while candidate.exists():
        candidate = root / f"{stem}-{index}"
        index += 1
    return candidate


async def run_from_cli(args) -> dict[str, Path]:
    """Run direct package evaluation from parsed CLI-like arguments.

    This resolves and checks the package, preflights its SPARQL endpoint, loads
    the dataset, builds effective runtime configuration from CLI overrides plus
    package defaults, runs the experiment, saves artifacts, and prints a summary.
    """
    package_dir = ensure_requested_package_is_active(args.package, settings.ontology_packages_path)
    package_settings = read_json_file(settings_path(package_dir))
    endpoint = package_settings.get("query_endpoint")
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise DomainError(f"Active package has no query_endpoint: {package_dir}")

    print(f"Preflight endpoint: {endpoint}")
    await preflight_endpoint(endpoint, timeout=args.preflight_timeout)

    dataset = load_dataset(args.dataset)
    output_dir = Path(args.output) if args.output else default_output_dir(package_dir, dataset.dataset_name)
    config = ExperimentConfig(
        package_dir=package_dir,
        model_name=args.model or "",
        retrieval_top_k=args.k or _int_setting(package_settings, "default_retrieval_top_k") or settings.runtime_retrieval_top_k,
        chunking_strategy=args.chunking or _string_setting(package_settings, "default_chunking_strategy") or "class_based",
        correction_max_iterations=args.corrections
        or _int_setting(package_settings, "correction_max_iterations")
        or settings.correction_max_iterations,
    )
    runner = ExperimentRunner(config)
    experiment, metrics = await runner.run_experiment(dataset, prefix_map=prefix_map_from_package(package_dir))
    saved = save_experiment(experiment, metrics, output_dir)

    print()
    print(format_experiment_report(experiment, metrics))
    print()
    print(f"Results: {saved['results']}")
    print(f"Metrics: {saved['metrics']}")
    print(f"Run config: {saved['run_config']}")
    print(f"Report:  {saved['report']}")
    return saved

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

