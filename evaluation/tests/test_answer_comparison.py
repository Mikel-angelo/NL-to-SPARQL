"""Tests for deterministic SPARQL answer comparison."""

from __future__ import annotations

import unittest

from evaluation.answer_comparison import (
    compare_results,
    normalize_literal,
    normalize_row,
    normalize_uri,
)


class AnswerComparisonTests(unittest.TestCase):
    def test_uri_normalization(self) -> None:
        self.assertEqual(normalize_uri("http://example.org/Foo"), "http://example.org/Foo")
        self.assertEqual(normalize_uri("<http://example.org/Foo>"), "http://example.org/Foo")
        self.assertEqual(normalize_uri("ex:Foo", {"ex": "http://example.org/"}), "http://example.org/Foo")
        self.assertEqual(normalize_uri("other:Bar", {"ex": "http://example.org/"}), "other:Bar")

    def test_literal_normalization(self) -> None:
        self.assertEqual(normalize_literal("42.0"), "42")
        self.assertEqual(normalize_literal('"hello"'), "hello")
        self.assertEqual(normalize_literal('"hello"@en'), "hello")
        self.assertEqual(normalize_literal('"42"^^xsd:integer'), "42")
        self.assertEqual(normalize_literal("True"), "true")
        self.assertEqual(normalize_literal("False"), "false")

    def test_row_normalization_ignores_variable_names(self) -> None:
        self.assertEqual(normalize_row({"name": "Alice", "age": "30"}), ("30", "Alice"))
        self.assertEqual(
            normalize_row({"entity": "ex:Foo"}, {"ex": "http://example.org/"}),
            ("http://example.org/Foo",),
        )
        self.assertEqual(normalize_row({"x": "hello", "y": "42"}), normalize_row({"a": "hello", "b": "42"}))

    def test_exact_match_ignores_order_and_variable_names(self) -> None:
        gold = [{"x": "http://example.org/A"}, {"x": "http://example.org/B"}]
        generated = [{"y": "http://example.org/B"}, {"y": "http://example.org/A"}]

        result = compare_results(generated, gold)

        self.assertTrue(result.exact_match)
        self.assertEqual(result.f1, 1.0)

    def test_partial_match(self) -> None:
        gold = [{"x": "A"}, {"x": "B"}, {"x": "C"}]
        generated = [{"y": "A"}, {"y": "B"}, {"y": "D"}]

        result = compare_results(generated, gold)

        self.assertFalse(result.exact_match)
        self.assertEqual(result.true_positives, 2)
        self.assertEqual(result.false_positives, 1)
        self.assertEqual(result.false_negatives, 1)
        self.assertAlmostEqual(result.precision, 2 / 3)
        self.assertAlmostEqual(result.recall, 2 / 3)

    def test_empty_and_none_cases(self) -> None:
        failed = compare_results(None, [{"x": "A"}])
        self.assertTrue(failed.generated_is_none)
        self.assertFalse(failed.exact_match)
        self.assertEqual(failed.precision, 1.0)
        self.assertEqual(failed.recall, 0.0)

        both_empty = compare_results([], [])
        self.assertTrue(both_empty.exact_match)
        self.assertEqual(both_empty.f1, 1.0)

        empty_generated = compare_results([], [{"x": "A"}])
        self.assertFalse(empty_generated.exact_match)
        self.assertEqual(empty_generated.precision, 1.0)
        self.assertEqual(empty_generated.recall, 0.0)

    def test_uri_and_numeric_normalization_in_comparison(self) -> None:
        uri_result = compare_results(
            [{"thing": "ex:Foo"}],
            [{"entity": "http://example.org/Foo"}],
            prefix_map={"ex": "http://example.org/"},
        )
        self.assertTrue(uri_result.exact_match)

        numeric_result = compare_results([{"total": "42.0"}], [{"count": "42"}])
        self.assertTrue(numeric_result.exact_match)

    def test_subset_and_superset_results(self) -> None:
        superset = compare_results([{"x": "A"}, {"x": "B"}, {"x": "C"}], [{"x": "A"}])
        self.assertFalse(superset.exact_match)
        self.assertEqual(superset.recall, 1.0)
        self.assertAlmostEqual(superset.precision, 1 / 3)

        subset = compare_results([{"x": "A"}], [{"x": "A"}, {"x": "B"}, {"x": "C"}])
        self.assertFalse(subset.exact_match)
        self.assertEqual(subset.precision, 1.0)
        self.assertAlmostEqual(subset.recall, 1 / 3)


if __name__ == "__main__":
    unittest.main()
