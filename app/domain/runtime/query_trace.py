"""Persist runtime query traces."""

from __future__ import annotations

from pathlib import Path
import json


def write_query_trace(path: Path, payload: dict[str, object]) -> Path:
    """Append one machine-readable runtime trace payload to `query.log`."""
    existing: list[dict[str, object]] = []
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, list):
                existing = [item for item in loaded if isinstance(item, dict)]
        except (OSError, json.JSONDecodeError):
            existing = []
    existing.append(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    return path


def write_readable_query_trace(
    *,
    latest_path: Path,
    runs_dir: Path,
    run_id: str,
    payload: dict[str, object],
) -> Path:
    """Write a compact plain-text query trace for humans."""
    text = render_readable_query_trace(payload)
    runs_dir.mkdir(parents=True, exist_ok=True)
    run_path = runs_dir / f"{run_id}.txt"
    run_path.write_text(text, encoding="utf-8")
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(text, encoding="utf-8")
    return run_path


def render_readable_query_trace(payload: dict[str, object]) -> str:
    """Render one query trace payload as compact plain text."""
    lines = [
        "QUERY RUN",
        f"Run ID: {_text(payload.get('run_id'))}",
        f"Run At: {_text(payload.get('run_at'))}",
        f"Status: {_text(payload.get('status'))}",
        f"Dataset: {_text(payload.get('dataset_name'))}",
        f"Endpoint: {_text(payload.get('dataset_endpoint'))}",
        f"Chunking: {_text(payload.get('chunking_strategy'))}",
        f"Retrieval top-k: {_text(payload.get('retrieval_top_k'))}",
        "",
        "QUESTION",
        _text(payload.get("question_asked")),
        "",
        "GENERATION PROMPT",
        _text(payload.get("prompt_generated")),
        "",
        "INITIAL GENERATED QUERY",
        _text(payload.get("llm_generated_query")),
        "",
        "ATTEMPTS",
    ]

    for item in _items(payload.get("correction_iterations")):
        lines.extend(
            [
                "",
                f"Iteration {item.get('iteration')}: {_text(item.get('status'))}",
                f"Validation: {_text(item.get('validation_summary'))}",
                "Query:",
                _text(item.get("query")),
            ]
        )
        errors = item.get("errors")
        if isinstance(errors, list) and errors:
            lines.append("Errors:")
            lines.extend(f"- {_text(error)}" for error in errors)
        execution = item.get("execution")
        if isinstance(execution, dict):
            lines.append(f"Execution: {_text(execution.get('code'))}")
        else:
            lines.append("Execution: not executed")

    lines.extend(
        [
            "",
            "FINAL QUERY",
            _text(payload.get("final_query")),
            "",
            "FINAL RESULTS",
            *_render_execution_result(payload.get("execution_result")),
            "",
            "FINAL ERRORS",
        ]
    )
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        lines.extend(f"- {_text(error)}" for error in errors)
    else:
        lines.append("- None")
    lines.append("")
    return "\n".join(lines)


def _render_execution_result(value: object) -> list[str]:
    if not isinstance(value, dict):
        return ["No execution result was recorded."]

    if "boolean" in value:
        return [f"ASK result: {_text(value.get('boolean'))}"]

    head = value.get("head")
    vars_ = head.get("vars") if isinstance(head, dict) else None
    results = value.get("results")
    bindings = results.get("bindings") if isinstance(results, dict) else None
    if not isinstance(vars_, list) or not isinstance(bindings, list):
        return [json.dumps(value, indent=2)]

    clean_vars = [_text(var) for var in vars_]
    lines = [f"Variables: {', '.join(clean_vars)}", f"Rows: {len(bindings)}"]
    for index, row in enumerate(bindings, 1):
        if not isinstance(row, dict):
            continue
        values = []
        for var in clean_vars:
            cell = row.get(var)
            if isinstance(cell, dict):
                values.append(f"{var}={_text(cell.get('value'))}")
            else:
                values.append(f"{var}=")
        lines.append(f"{index}. " + " | ".join(values))
    return lines


def _items(value: object) -> list[dict[str, object]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _text(value: object) -> str:
    return "" if value is None else str(value)
