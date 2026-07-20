"""Run the full evaluation matrix: ontologies x models x repetitions.

This wraps the existing `evaluate.py` CLI, calling it once per
(ontology, model, repetition) combination. It then parses each produced
results.json, computes Execution Accuracy (EA) and related metrics, and writes:

  1. results_matrix_runs.csv   - one row per individual run
  2. results_matrix_summary.csv - one row per (ontology, model) with mean +/- std
  3. A printed summary table

Usage:
    python run_experiment_matrix.py

Edit the CONFIG section below to match your package timestamps and model list.
Nothing here is model- or ontology-specific beyond that config block, so it can
be reused as you add ontologies or models.
"""

from __future__ import annotations

import json
import statistics
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


# ============================ CONFIG ============================
# Fill in the package directory for each ontology (the onboarded package,
# NOT the dataset). Timestamps will differ on your machine - update them.

PYTHON = sys.executable  # uses the same interpreter running this script

EVALUATE_SCRIPT = "evaluate.py"
ACTIVATE_SCRIPT = "activate.py"

# Each ontology: friendly name -> (dataset path, package dir)
ONTOLOGIES: dict[str, tuple[str, str]] = {
    "bestiary": (
        r"evaluation\datasets\bestiary_sandro_eval_dataset.json",
        r"ontology_packages\bestiary-20260703-1024",
    ),
}

MODELS: list[str] = [
#    "llama3.1:8b",
    "deepseek-r1:32b",
]

REPETITIONS = 3

CHUNKING = "composite"

# Where evaluate.py writes results. It defaults to
# <package>/evaluation/<dataset-stem>-<runid>/results.json
# We locate the newest results.json under the package after each run.
# ===============================================================


@dataclass
class RunResult:
    ontology: str
    model: str
    repetition: int
    ea: float
    exact: int
    scored: int
    empty: int
    partial: int
    corrections_helped: int
    avg_latency_ms: float
    results_path: str
    ok: bool = True
    error: str = ""


def main() -> None:
    started = datetime.now()
    print(f"Experiment matrix started: {started:%Y-%m-%d %H:%M:%S}")
    print(f"Ontologies: {len(ONTOLOGIES)}  Models: {len(MODELS)}  Reps: {REPETITIONS}")
    total_runs = len(ONTOLOGIES) * len(MODELS) * REPETITIONS
    print(f"Total runs: {total_runs}\n")

    runs: list[RunResult] = []
    run_index = 0

    for ont_name, (dataset, package) in ONTOLOGIES.items():
        # Activate this package once before running all its model/rep combinations.
        # Activation sets the Fuseki dataset as active; it is per-package, so we
        # do it once per ontology rather than once per run.
        print(f"\nActivating package for '{ont_name}' ...", end=" ", flush=True)
        activated = _activate_package(package)
        if not activated:
            print("FAILED - skipping this ontology's runs")
            # Record failures for all runs of this ontology so the matrix is complete
            for model in MODELS:
                for rep in range(1, REPETITIONS + 1):
                    run_index += 1
                    runs.append(
                        RunResult(ont_name, model, rep, 0, 0, 0, 0, 0, 0, 0.0, "", False, "activation failed")
                    )
            continue
        print("ok")

        for model in MODELS:
            for rep in range(1, REPETITIONS + 1):
                run_index += 1
                tag = f"[{run_index}/{total_runs}] {ont_name} | {model} | rep {rep}"
                print(f"{tag} ... ", end="", flush=True)

                result = _run_single(ont_name, dataset, package, model, rep)
                runs.append(result)

                if result.ok:
                    print(f"EA={result.ea:.1f}%  ({result.exact}/{result.scored})")
                else:
                    print(f"FAILED: {result.error}")

    _write_runs_csv(runs)
    summary = _summarize(runs)
    _write_summary_csv(summary)
    _print_summary(summary)

    elapsed = datetime.now() - started
    print(f"\nDone in {elapsed}. Wrote results_matrix_runs.csv and results_matrix_summary.csv")


def _activate_package(package: str) -> bool:
    """Activate one ontology package via activate.py. Returns True on success."""
    cmd = [PYTHON, ACTIVATE_SCRIPT, "--package", package]
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        return False
    return completed.returncode == 0


def _run_single(
    ont_name: str,
    dataset: str,
    package: str,
    model: str,
    rep: int,
) -> RunResult:
    """Invoke evaluate.py once and parse its results.json."""
    import time
    run_started = time.time()

    cmd = [
        PYTHON,
        EVALUATE_SCRIPT,
        "--dataset", dataset,
        "--package", package,
        "--model", model,
        "--chunking", CHUNKING,
    ]

    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200,  # 2 hours per run ceiling
        )
    except subprocess.TimeoutExpired:
        return RunResult(ont_name, model, rep, 0, 0, 0, 0, 0, 0, 0.0, "", False, "timeout")

    if completed.returncode != 0:
        stderr = (completed.stderr or "unknown error").strip()
        err = stderr.splitlines()[-1][:120] if stderr.splitlines() else "unknown error"
        return RunResult(ont_name, model, rep, 0, 0, 0, 0, 0, 0, 0.0, "", False, err)

    results_path = _find_latest_results(Path(package), after=run_started)
    if results_path is None:
        return RunResult(ont_name, model, rep, 0, 0, 0, 0, 0, 0, 0.0, "", False, "no results.json found")

    try:
        metrics = _parse_results(results_path)
    except Exception as exc:
        return RunResult(
            ont_name, model, rep, 0, 0, 0, 0, 0, 0, 0.0, str(results_path), False,
            f"parse error: {str(exc)[:80]}",
        )
    return RunResult(
        ontology=ont_name,
        model=model,
        repetition=rep,
        results_path=str(results_path),
        **metrics,
    )


def _find_latest_results(package_dir: Path, after: float | None = None) -> Path | None:
    """Find the most recently modified results.json under the package.

    If `after` is given, only accept files modified at or after that time, so a
    stale results.json from a previous run is not mistaken for the current one.
    """
    eval_dir = package_dir / "evaluation"
    if not eval_dir.exists():
        return None
    candidates = list(eval_dir.rglob("results.json"))
    if after is not None:
        candidates = [p for p in candidates if p.stat().st_mtime >= after - 1]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _parse_results(results_path: Path) -> dict:
    """Compute EA and related metrics from a results.json file."""
    data = json.loads(results_path.read_text(encoding="utf-8"))
    results = data if isinstance(data, list) else data.get("results", data.get("questions", []))

    scored = 0
    exact = 0
    empty = 0
    partial = 0
    corrections_helped = 0
    latency_total = 0.0
    latency_count = 0

    for r in results:
        gold = r.get("gold_answers", [])
        gen = r.get("final_answers") or []
        iters = r.get("total_iterations", 1) or 1
        lat = r.get("total_latency_ms", 0) or 0
        latency_total += lat
        latency_count += 1

        if len(gold) > 0:
            scored += 1
        if len(gen) > 0 and len(gen) == len(gold) and len(gold) > 0:
            exact += 1
            if iters > 1:
                corrections_helped += 1
        elif len(gen) == 0 and len(gold) > 0:
            empty += 1
        elif len(gen) > 0 and len(gold) > 0:
            partial += 1

    ea = (exact / scored * 100) if scored > 0 else 0.0
    avg_latency = (latency_total / latency_count) if latency_count else 0.0

    return {
        "ea": ea,
        "exact": exact,
        "scored": scored,
        "empty": empty,
        "partial": partial,
        "corrections_helped": corrections_helped,
        "avg_latency_ms": avg_latency,
    }


@dataclass
class SummaryRow:
    ontology: str
    model: str
    ea_mean: float
    ea_std: float
    ea_min: float
    ea_max: float
    reps: int
    scored: int
    corrections_helped_mean: float
    avg_latency_ms: float
    eas: list[float] = field(default_factory=list)


def _summarize(runs: list[RunResult]) -> list[SummaryRow]:
    """Aggregate repetitions into mean +/- std per (ontology, model)."""
    grouped: dict[tuple[str, str], list[RunResult]] = {}
    for run in runs:
        if not run.ok:
            continue
        grouped.setdefault((run.ontology, run.model), []).append(run)

    summary: list[SummaryRow] = []
    for (ont, model), group in grouped.items():
        eas = [g.ea for g in group]
        summary.append(
            SummaryRow(
                ontology=ont,
                model=model,
                ea_mean=statistics.mean(eas),
                ea_std=statistics.pstdev(eas) if len(eas) > 1 else 0.0,
                ea_min=min(eas),
                ea_max=max(eas),
                reps=len(group),
                scored=group[0].scored,
                corrections_helped_mean=statistics.mean(g.corrections_helped for g in group),
                avg_latency_ms=statistics.mean(g.avg_latency_ms for g in group),
                eas=eas,
            )
        )
    return summary


def _write_runs_csv(runs: list[RunResult]) -> None:
    lines = [
        "ontology,model,repetition,ea,exact,scored,empty,partial,corrections_helped,avg_latency_ms,ok,error,results_path"
    ]
    for r in runs:
        lines.append(
            f"{r.ontology},{r.model},{r.repetition},{r.ea:.2f},{r.exact},{r.scored},"
            f"{r.empty},{r.partial},{r.corrections_helped},{r.avg_latency_ms:.0f},"
            f"{r.ok},{r.error},{r.results_path}"
        )
    Path("results_matrix_runs.csv").write_text("\n".join(lines), encoding="utf-8")


def _write_summary_csv(summary: list[SummaryRow]) -> None:
    lines = [
        "ontology,model,ea_mean,ea_std,ea_min,ea_max,reps,scored,corrections_helped_mean,avg_latency_ms"
    ]
    for s in sorted(summary, key=lambda x: (x.ontology, x.model)):
        lines.append(
            f"{s.ontology},{s.model},{s.ea_mean:.2f},{s.ea_std:.2f},{s.ea_min:.2f},"
            f"{s.ea_max:.2f},{s.reps},{s.scored},{s.corrections_helped_mean:.1f},"
            f"{s.avg_latency_ms:.0f}"
        )
    Path("results_matrix_summary.csv").write_text("\n".join(lines), encoding="utf-8")


def _print_summary(summary: list[SummaryRow]) -> None:
    print("\n" + "=" * 78)
    print("SUMMARY (Execution Accuracy, mean +/- std over repetitions)")
    print("=" * 78)
    print(f"{'Ontology':16s} {'Model':22s} {'EA mean':>9s} {'std':>6s} {'range':>13s}")
    print("-" * 78)
    for s in sorted(summary, key=lambda x: (x.ontology, x.model)):
        rng = f"{s.ea_min:.1f}-{s.ea_max:.1f}"
        print(f"{s.ontology:16s} {s.model:22s} {s.ea_mean:8.1f}% {s.ea_std:5.1f} {rng:>13s}")


if __name__ == "__main__":
    main()
