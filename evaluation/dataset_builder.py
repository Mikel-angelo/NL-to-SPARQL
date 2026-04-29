"""
Dataset Builder Utility
=======================
Helps create and validate evaluation datasets by executing gold SPARQL
queries against a Fuseki endpoint and capturing the result sets.

Usage:
    # Populate gold answers for a dataset (fills in gold_answers from Fuseki)
    python -m evaluation.dataset_builder \
        --dataset evaluation/datasets/enovation_v1.json \
        --endpoint http://127.0.0.1:3030/my-dataset/query \
        --populate

    # Validate that all gold SPARQL queries execute successfully
    python -m evaluation.dataset_builder \
        --dataset evaluation/datasets/enovation_v1.json \
        --endpoint http://127.0.0.1:3030/my-dataset/query \
        --validate
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import httpx


def execute_sparql(endpoint: str, sparql: str, timeout: int = 30) -> dict:
    """Execute a SPARQL query against a Fuseki endpoint and return raw results."""
    response = httpx.post(
        endpoint,
        data={"query": sparql},
        headers={"Accept": "application/sparql-results+json"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def extract_bindings(raw_result: dict) -> list[dict[str, str]]:
    """Extract simplified bindings from a Fuseki SPARQL JSON response."""
    bindings = raw_result.get("results", {}).get("bindings", [])
    return [
        {var: binding[var]["value"] for var in binding}
        for binding in bindings
    ]


def populate_gold_answers(dataset_path: str, endpoint: str, timeout: int = 30):
    """
    Execute all gold SPARQL queries and fill in the gold_answers field.
    Saves the updated dataset back to the same file.
    """
    path = Path(dataset_path)
    dataset = json.loads(path.read_text(encoding="utf-8"))

    questions = dataset.get("questions", [])
    total = len(questions)
    success = 0
    errors = []

    for idx, q in enumerate(questions, 1):
        qid = q.get("id", f"Q{idx}")
        sparql = q.get("gold_sparql", "")

        if not sparql.strip():
            print(f"  [{idx}/{total}] {qid}: SKIP (no gold SPARQL)")
            continue

        print(f"  [{idx}/{total}] {qid}: Executing...", end=" ")
        try:
            raw = execute_sparql(endpoint, sparql, timeout)
            answers = extract_bindings(raw)
            q["gold_answers"] = answers
            print(f"OK ({len(answers)} rows)")
            success += 1
        except httpx.HTTPStatusError as exc:
            msg = f"HTTP {exc.response.status_code}"
            print(f"FAIL ({msg})")
            errors.append((qid, msg))
        except Exception as exc:
            msg = str(exc)[:100]
            print(f"FAIL ({msg})")
            errors.append((qid, msg))

    # Save updated dataset
    path.write_text(
        json.dumps(dataset, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"\nDone: {success}/{total} populated, {len(errors)} errors")
    if errors:
        print("Errors:")
        for qid, msg in errors:
            print(f"  {qid}: {msg}")


def validate_dataset(dataset_path: str, endpoint: str, timeout: int = 30):
    """
    Validate all gold SPARQL queries: check they parse, execute, and return
    the expected gold_answers.
    """
    path = Path(dataset_path)
    dataset = json.loads(path.read_text(encoding="utf-8"))

    questions = dataset.get("questions", [])
    total = len(questions)
    passed = 0
    issues = []

    for idx, q in enumerate(questions, 1):
        qid = q.get("id", f"Q{idx}")
        sparql = q.get("gold_sparql", "")
        expected = q.get("gold_answers", [])

        if not sparql.strip():
            issues.append((qid, "No gold SPARQL"))
            continue

        print(f"  [{idx}/{total}] {qid}: ", end="")

        try:
            raw = execute_sparql(endpoint, sparql, timeout)
            actual = extract_bindings(raw)

            if not expected:
                print(f"WARN (no gold_answers to compare, got {len(actual)} rows)")
                issues.append((qid, f"No gold_answers set (query returns {len(actual)} rows)"))
            elif len(actual) != len(expected):
                print(f"MISMATCH (expected {len(expected)} rows, got {len(actual)})")
                issues.append((qid, f"Row count mismatch: expected {len(expected)}, got {len(actual)}"))
            else:
                # Simple value comparison (not using full normalization here,
                # just checking the raw values match)
                actual_sets = {tuple(sorted(r.values())) for r in actual}
                expected_sets = {tuple(sorted(r.values())) for r in expected}
                if actual_sets == expected_sets:
                    print("OK")
                    passed += 1
                else:
                    print("MISMATCH (values differ)")
                    issues.append((qid, "Value mismatch"))

        except httpx.HTTPStatusError as exc:
            msg = f"HTTP {exc.response.status_code}"
            print(f"FAIL ({msg})")
            issues.append((qid, f"Execution failed: {msg}"))
        except Exception as exc:
            msg = str(exc)[:100]
            print(f"FAIL ({msg})")
            issues.append((qid, f"Error: {msg}"))

    print(f"\nValidation: {passed}/{total} passed")
    if issues:
        print(f"Issues ({len(issues)}):")
        for qid, msg in issues:
            print(f"  {qid}: {msg}")


def show_stats(dataset_path: str):
    """Print dataset statistics."""
    path = Path(dataset_path)
    dataset = json.loads(path.read_text(encoding="utf-8"))
    questions = dataset.get("questions", [])

    print(f"Dataset: {dataset.get('dataset_name', 'unnamed')}")
    print(f"Ontology: {dataset.get('ontology_file', 'unknown')}")
    print(f"Total questions: {len(questions)}")

    # Complexity distribution
    tiers = {}
    for q in questions:
        t = q.get("complexity_tier", "unknown")
        tiers[t] = tiers.get(t, 0) + 1
    print(f"\nBy complexity: {tiers}")

    # Shape distribution
    shapes = {}
    for q in questions:
        s = q.get("query_shape", "unknown")
        shapes[s] = shapes.get(s, 0) + 1
    print(f"By shape: {shapes}")

    # Type distribution
    types = {}
    for q in questions:
        t = q.get("question_type", "unknown")
        types[t] = types.get(t, 0) + 1
    print(f"By type: {types}")

    # Gold answers coverage
    with_answers = sum(1 for q in questions if q.get("gold_answers"))
    print(f"\nWith gold answers: {with_answers}/{len(questions)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluation dataset builder")
    parser.add_argument("--dataset", type=str, required=True, help="Path to dataset JSON")
    parser.add_argument("--endpoint", type=str, help="Fuseki SPARQL endpoint URL")
    parser.add_argument("--timeout", type=int, default=30, help="Query timeout in seconds")

    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--populate", action="store_true", help="Execute gold SPARQL and fill gold_answers")
    action.add_argument("--validate", action="store_true", help="Validate gold SPARQL against endpoint")
    action.add_argument("--stats", action="store_true", help="Show dataset statistics")

    args = parser.parse_args()

    if args.populate:
        if not args.endpoint:
            print("Error: --endpoint required for --populate")
        else:
            populate_gold_answers(args.dataset, args.endpoint, args.timeout)
    elif args.validate:
        if not args.endpoint:
            print("Error: --endpoint required for --validate")
        else:
            validate_dataset(args.dataset, args.endpoint, args.timeout)
    elif args.stats:
        show_stats(args.dataset)
