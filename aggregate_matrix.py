"""Aggregate all evaluation runs on disk into one results matrix.

Walks every ontology package under ONTOLOGY_PACKAGES_ROOT, finds each run folder
(which contains a `run_config.json` next to a `results.json`), reads the model
name and dataset from the config, computes Execution Accuracy (EA) and related
metrics from the results, and groups everything by (ontology, model).

Unlike the live runner, this depends on nothing but the files already on disk,
so it recovers cleanly from any interrupted or stop-start experiment history.
It automatically picks up new runs (e.g. the remaining Bestiary ones) the next
time you run it.

Outputs:
  matrix_runs.csv     - one row per individual run found on disk
  matrix_summary.csv  - one row per (ontology, model): mean/std/min/max EA
  A printed EA matrix (ontologies x models)

Usage:
    python aggregate_matrix.py
    python aggregate_matrix.py --packages-root ontology_packages
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


# Map dataset_name (from run_config.json) to a short friendly ontology label.
# Extend this if you add ontologies. Anything unmatched falls back to the raw
# dataset name, so nothing is ever silently dropped.
DATASET_TO_ONTOLOGY = {
    "eNOVATION_eval_dataset": "eNOVATION",
    "spider_dog_kennels_combined_eval_dataset": "Dog Kennels",
    "spider_concert_singer_combined_eval_dataset": "Concert Singer",
    "spider_world_1_combined_eval_dataset": "World",
    "bestiary_sandro_eval_dataset": "Bestiary",
}

# Column order for the printed matrix. Models not listed here still appear,
# appended after these in discovery order.
MODEL_ORDER = [
    "qwen2.5-coder:7b",
    "llama3.1:8b",
    "qwen2.5-coder:32b",
    "deepseek-r1:32b",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--packages-root", default="ontology_packages")
    args = parser.parse_args()

    root = Path(args.packages_root)
    if not root.exists():
        print(f"Packages root not found: {root.resolve()}")
        return

    runs = _scan_runs(root)
    if not runs:
        print("No runs found. Check the packages root path.")
        return

    print(f"Found {len(runs)} runs on disk.\n")

    _write_runs_csv(runs)
    summary = _summarize(runs)
    _write_summary_csv(summary)
    _print_matrix(summary)
    _print_summary(summary)
    print("\nWrote matrix_runs.csv and matrix_summary.csv")


class Run:
    def __init__(self, ontology, model, ea, exact, scored, empty, partial,
                 corrections_helped, avg_latency_ms, path):
        self.ontology = ontology
        self.model = model
        self.ea = ea
        self.exact = exact
        self.scored = scored
        self.empty = empty
        self.partial = partial
        self.corrections_helped = corrections_helped
        self.avg_latency_ms = avg_latency_ms
        self.path = path


def _scan_runs(root: Path) -> list[Run]:
    """Find every run_config.json + results.json pair under the packages root."""
    runs: list[Run] = []
    for config_path in root.rglob("run_config.json"):
        results_path = config_path.parent / "results.json"
        if not results_path.exists():
            continue
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue

        model = config.get("model_name", "unknown")
        dataset = config.get("dataset_name", "unknown")
        ontology = DATASET_TO_ONTOLOGY.get(dataset, dataset)

        try:
            metrics = _parse_results(results_path)
        except Exception:
            continue

        runs.append(Run(
            ontology=ontology,
            model=model,
            path=str(results_path),
            **metrics,
        ))
    return runs


def _parse_results(results_path: Path) -> dict:
    data = json.loads(results_path.read_text(encoding="utf-8"))
    results = data if isinstance(data, list) else data.get("results", data.get("questions", []))

    scored = exact = empty = partial = corrections_helped = 0
    latency_total = 0.0
    latency_count = 0

    for r in results:
        gold = r.get("gold_answers", [])
        gen = r.get("final_answers") or []
        iters = r.get("total_iterations", 1) or 1
        latency_total += r.get("total_latency_ms", 0) or 0
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


class SummaryRow:
    def __init__(self, ontology, model, eas, scored, corr, latency):
        self.ontology = ontology
        self.model = model
        self.eas = eas
        self.reps = len(eas)
        self.ea_mean = statistics.mean(eas)
        self.ea_std = statistics.pstdev(eas) if len(eas) > 1 else 0.0
        self.ea_min = min(eas)
        self.ea_max = max(eas)
        self.scored = scored
        self.corrections_helped_mean = statistics.mean(corr) if corr else 0.0
        self.avg_latency_ms = statistics.mean(latency) if latency else 0.0


def _summarize(runs: list[Run]) -> list[SummaryRow]:
    grouped: dict[tuple[str, str], list[Run]] = defaultdict(list)
    for run in runs:
        grouped[(run.ontology, run.model)].append(run)

    summary = []
    for (ont, model), group in grouped.items():
        summary.append(SummaryRow(
            ontology=ont,
            model=model,
            eas=[g.ea for g in group],
            scored=group[0].scored,
            corr=[g.corrections_helped for g in group],
            latency=[g.avg_latency_ms for g in group],
        ))
    return summary


def _ordered_models(summary: list[SummaryRow]) -> list[str]:
    present = {s.model for s in summary}
    ordered = [m for m in MODEL_ORDER if m in present]
    extras = sorted(present - set(ordered))
    return ordered + extras


def _ordered_ontologies(summary: list[SummaryRow]) -> list[str]:
    present = {s.ontology for s in summary}
    preferred = ["eNOVATION", "Dog Kennels", "Concert Singer", "World", "Bestiary"]
    ordered = [o for o in preferred if o in present]
    extras = sorted(present - set(ordered))
    return ordered + extras


def _write_runs_csv(runs: list[Run]) -> None:
    lines = ["ontology,model,ea,exact,scored,empty,partial,corrections_helped,avg_latency_ms,results_path"]
    for r in sorted(runs, key=lambda x: (x.ontology, x.model, x.path)):
        lines.append(
            f"{r.ontology},{r.model},{r.ea:.2f},{r.exact},{r.scored},{r.empty},"
            f"{r.partial},{r.corrections_helped},{r.avg_latency_ms:.0f},{r.path}"
        )
    Path("matrix_runs.csv").write_text("\n".join(lines), encoding="utf-8")


def _write_summary_csv(summary: list[SummaryRow]) -> None:
    lines = ["ontology,model,ea_mean,ea_std,ea_min,ea_max,reps,scored,corrections_helped_mean,avg_latency_ms"]
    for s in sorted(summary, key=lambda x: (x.ontology, x.model)):
        lines.append(
            f"{s.ontology},{s.model},{s.ea_mean:.2f},{s.ea_std:.2f},{s.ea_min:.2f},"
            f"{s.ea_max:.2f},{s.reps},{s.scored},{s.corrections_helped_mean:.1f},"
            f"{s.avg_latency_ms:.0f}"
        )
    Path("matrix_summary.csv").write_text("\n".join(lines), encoding="utf-8")


def _print_matrix(summary: list[SummaryRow]) -> None:
    models = _ordered_models(summary)
    ontologies = _ordered_ontologies(summary)
    lookup = {(s.ontology, s.model): s for s in summary}

    col_w = 18
    print("=" * (20 + col_w * len(models)))
    print("EA MATRIX (mean% +/- std over repetitions)")
    print("=" * (20 + col_w * len(models)))

    header = f"{'Ontology':20s}"
    for m in models:
        header += f"{_short_model(m):>{col_w}s}"
    print(header)
    print("-" * (20 + col_w * len(models)))

    for ont in ontologies:
        row = f"{ont:20s}"
        for m in models:
            s = lookup.get((ont, m))
            if s is None:
                row += f"{'-':>{col_w}s}"
            else:
                cell = f"{s.ea_mean:.1f}+/-{s.ea_std:.1f}"
                row += f"{cell:>{col_w}s}"
        print(row)


def _print_summary(summary: list[SummaryRow]) -> None:
    print("\n" + "=" * 84)
    print("DETAIL (per ontology x model)")
    print("=" * 84)
    print(f"{'Ontology':16s} {'Model':22s} {'EA mean':>9s} {'std':>6s} {'range':>13s} {'reps':>5s}")
    print("-" * 84)
    for s in sorted(summary, key=lambda x: (_ont_key(x.ontology), x.model)):
        rng = f"{s.ea_min:.1f}-{s.ea_max:.1f}"
        print(f"{s.ontology:16s} {s.model:22s} {s.ea_mean:8.1f}% {s.ea_std:5.1f} {rng:>13s} {s.reps:5d}")


def _short_model(model: str) -> str:
    return (model.replace("qwen2.5-coder:", "qwen-")
                 .replace("llama3.1:", "llama-")
                 .replace("deepseek-r1:", "dsr1-"))


def _ont_key(ontology: str) -> int:
    order = ["eNOVATION", "Dog Kennels", "Concert Singer", "World", "Bestiary"]
    return order.index(ontology) if ontology in order else 99


if __name__ == "__main__":
    main()
