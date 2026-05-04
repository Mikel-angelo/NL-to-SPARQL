"""Compare generated SPARQL answers with gold answers.

The runtime and the gold query can return the same logical answer with small
surface differences: URI values may be written as full IRIs or prefixed names,
typed literals may include datatype suffixes, numbers may differ only by
formatting, and result variables may have different names. This module
normalizes those cases before scoring.

The public entry point is `compare_results()`. It returns exact match,
precision, recall, F1, and the missing/extra normalized rows used by evaluation
reports. It deliberately does not execute SPARQL or know about datasets; it only
compares already-materialized result rows.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional


def normalize_uri(value: str, prefix_map: Optional[dict[str, str]] = None) -> str:
    """Return a canonical URI string for full IRIs, angle-bracket IRIs, or prefixed names.

    `prefix_map` maps prefixes such as `ex` to namespaces. When provided,
    `ex:Thing` and `http://example/.../Thing` can compare equal. Unknown
    prefixes are left unchanged so mismatches remain visible in the diff.
    """
    value = value.strip()
    if value.startswith("<") and value.endswith(">"):
        value = value[1:-1]

    if prefix_map and ":" in value and not value.startswith("http"):
        prefix, _, local = value.partition(":")
        if prefix in prefix_map:
            value = prefix_map[prefix] + local
    return value


def normalize_literal(value: str) -> str:
    """Return a canonical literal string for answer comparison.

    This removes common RDF literal wrappers, language tags, and datatype
    suffixes, then normalizes numeric and boolean spellings. The goal is to
    avoid penalizing harmless serialization differences while preserving the
    literal's logical value.
    """
    value = value.strip()

    datatype_match = re.match(r'^"?(.*?)"?\^\^<?[^>]+>?$', value)
    if datatype_match:
        value = datatype_match.group(1)

    lang_match = re.match(r'^"?(.*?)"?@[a-zA-Z\-]+$', value)
    if lang_match:
        value = lang_match.group(1)

    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        value = value[1:-1]

    try:
        number = float(value)
        if number == int(number):
            return str(int(number))
        return str(number)
    except (ValueError, OverflowError):
        pass

    if value.lower() in ("true", "yes"):
        return "true"
    if value.lower() in ("false", "no"):
        return "false"
    return value.strip()


def normalize_value(value: str, prefix_map: Optional[dict[str, str]] = None) -> str:
    """Normalize one SPARQL result cell as either a URI-like value or a literal."""
    value = value.strip()
    is_uri = (
        value.startswith("http://")
        or value.startswith("https://")
        or (value.startswith("<") and value.endswith(">"))
    )

    if not is_uri and prefix_map and ":" in value:
        is_uri = value.split(":", 1)[0] in prefix_map

    if is_uri:
        return normalize_uri(value, prefix_map)
    return normalize_literal(value)


def normalize_row(
    row: dict[str, str],
    prefix_map: Optional[dict[str, str]] = None,
) -> tuple[str, ...]:
    """Normalize one result row into a sorted tuple of values.

    Variable names are intentionally ignored. For evaluation, a row containing
    `?x = Alice` is treated the same as a row containing `?label = Alice`.
    Values are sorted so column order does not affect equality.
    """
    return tuple(sorted(normalize_value(str(value), prefix_map) for value in row.values()))


def normalize_result_set(
    results: list[dict[str, str]],
    prefix_map: Optional[dict[str, str]] = None,
) -> set[tuple[str, ...]]:
    """Normalize all result rows into a set suitable for exact and partial matching."""
    return {normalize_row(row, prefix_map) for row in results}


@dataclass
class ComparisonResult:
    """Scoring details for one generated-vs-gold answer comparison.

    `exact_match` is true only when no normalized rows are missing or extra.
    Precision/recall/F1 are computed over normalized row sets. `missing_rows`
    and `extra_rows` are stored for readable evaluation logs.
    """

    exact_match: bool = False
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0

    gold_size: int = 0
    generated_size: int = 0
    true_positives: int = 0
    false_positives: int = 0
    false_negatives: int = 0

    gold_is_empty: bool = False
    generated_is_empty: bool = False
    generated_is_none: bool = False

    missing_rows: list[tuple] = field(default_factory=list)
    extra_rows: list[tuple] = field(default_factory=list)


def compare_results(
    generated: Optional[list[dict[str, str]]],
    gold: list[dict[str, str]],
    prefix_map: Optional[dict[str, str]] = None,
) -> ComparisonResult:
    """Compare generated and gold result sets using normalized row-set overlap.

    `generated=None` represents a pipeline failure or non-result, while an empty
    list represents a successful query that returned no rows. Empty gold answers
    are handled explicitly so unscored questions can still be represented
    consistently by the caller.
    """
    result = ComparisonResult(gold_is_empty=len(gold) == 0)

    if generated is None:
        result.generated_is_none = True
        result.generated_is_empty = True
        if result.gold_is_empty:
            result.exact_match = True
            result.precision = 1.0
            result.recall = 1.0
            result.f1 = 1.0
        else:
            result.precision = 1.0
            result.recall = 0.0
            result.f1 = 0.0
            result.gold_size = len(gold)
            result.false_negatives = len(gold)
        return result

    result.generated_is_empty = len(generated) == 0

    if result.gold_is_empty and result.generated_is_empty:
        result.exact_match = True
        result.precision = 1.0
        result.recall = 1.0
        result.f1 = 1.0
        return result

    if result.gold_is_empty and not result.generated_is_empty:
        result.generated_size = len(generated)
        result.false_positives = len(generated)
        result.precision = 0.0
        result.recall = 1.0
        result.f1 = 0.0
        return result

    if result.generated_is_empty and not result.gold_is_empty:
        result.gold_size = len(gold)
        result.false_negatives = len(gold)
        result.precision = 1.0
        result.recall = 0.0
        result.f1 = 0.0
        return result

    gold_set = normalize_result_set(gold, prefix_map)
    generated_set = normalize_result_set(generated, prefix_map)

    result.gold_size = len(gold_set)
    result.generated_size = len(generated_set)

    true_positives = gold_set & generated_set
    false_positives = generated_set - gold_set
    false_negatives = gold_set - generated_set

    result.true_positives = len(true_positives)
    result.false_positives = len(false_positives)
    result.false_negatives = len(false_negatives)
    result.missing_rows = sorted(false_negatives)
    result.extra_rows = sorted(false_positives)
    result.exact_match = not false_positives and not false_negatives

    result.precision = len(true_positives) / len(generated_set) if generated_set else 0.0
    result.recall = len(true_positives) / len(gold_set) if gold_set else 0.0
    if result.precision + result.recall > 0:
        result.f1 = 2 * result.precision * result.recall / (result.precision + result.recall)
    return result
