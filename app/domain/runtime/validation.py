"""Formal validation stages for generated SPARQL.

This module validates a candidate query before endpoint execution. It normalizes
missing ontology prefixes, checks SPARQL parser syntax, verifies prefixes and
ontology vocabulary references against `ontology_context.json`, and applies a
small set of structural checks. It does not call the LLM, render prompts, or
execute queries against a SPARQL endpoint.
"""

from __future__ import annotations

from dataclasses import dataclass
import re

from rdflib.plugins.sparql.parser import parseQuery


@dataclass(frozen=True)
class ValidationStageResult:
    """Result for one named validation stage.

    `code` is intended for stable programmatic traces, while `message` is a
    human-readable explanation included only when the stage fails.
    """

    stage: str
    passed: bool
    code: str
    message: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "stage": self.stage,
            "passed": self.passed,
            "code": self.code,
            "message": self.message,
        }


@dataclass(frozen=True)
class QueryValidationResult:
    """Validation outcome for one candidate SPARQL query.

    `normalized_query` includes ontology prefixes injected from the package
    context when they were missing from the generated query. `errors` is the
    ordered list of failed-stage messages/codes passed to correction prompts.
    """

    is_valid: bool
    errors: list[str]
    normalized_query: str
    stages: list[ValidationStageResult]

    def to_dict(self) -> dict[str, object]:
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "normalized_query": self.normalized_query,
            "stages": [stage.to_dict() for stage in self.stages],
        }


def validate_query(
    query: str,
    *,
    ontology_context: dict[str, object] | None = None,
) -> QueryValidationResult:
    """Run all formal validation stages for one candidate query.

    The stages are intentionally deterministic and local: syntax parsing,
    prefix availability, vocabulary membership, and structural sanity checks.
    Endpoint execution is handled later by the runtime pipeline.
    """
    context = ontology_context or {}
    normalized_query = _normalized_query(query, context)
    stages = [
        _syntactic_validation(normalized_query),
        _prefix_validation(normalized_query, context),
        _vocabulary_validation(normalized_query, context),
        _structural_validation(normalized_query, context),
    ]
    errors = [stage.message or stage.code for stage in stages if not stage.passed]
    return QueryValidationResult(
        is_valid=not errors,
        errors=errors,
        normalized_query=normalized_query,
        stages=stages,
    )


def _syntactic_validation(query: str) -> ValidationStageResult:
    if not query:
        return _fail("syntactic", "SPARQL_EMPTY", "Generated query is empty")
    try:
        parseQuery(query)
    except Exception as exc:
        return _fail("syntactic", "SPARQL_PARSE_ERROR", f"SPARQL parser error: {exc}")
    return _pass("syntactic", "SPARQL_PARSE_OK")


def _prefix_validation(query: str, ontology_context: dict[str, object]) -> ValidationStageResult:
    declared = _declared_prefixes(query)
    available = _ontology_prefixes(ontology_context)
    used = _used_prefixes(query)
    missing = sorted(prefix for prefix in used if prefix not in declared and prefix not in available)
    if missing:
        return _fail(
            "prefix",
            "PREFIX_UNKNOWN",
            f"Query uses undeclared prefixes: {', '.join(missing)}",
        )
    return _pass("prefix", "PREFIXES_OK")


def _vocabulary_validation(query: str, ontology_context: dict[str, object]) -> ValidationStageResult:
    class_uris, property_uris = _ontology_vocabulary(ontology_context)
    prefix_map = {**_ontology_prefixes(ontology_context), **_declared_prefixes(query)}
    unknown: list[str] = []

    for predicate_uri in _predicate_uris(query, prefix_map):
        if _is_builtin_predicate(predicate_uri):
            continue
        if predicate_uri not in property_uris:
            unknown.append(predicate_uri)

    for class_uri in _rdf_type_object_uris(query, prefix_map):
        if class_uri not in class_uris:
            unknown.append(class_uri)

    if unknown:
        return _fail(
            "vocabulary",
            "VOCABULARY_UNKNOWN_URI",
            f"Query references unknown ontology class/property URIs: {', '.join(sorted(set(unknown)))}",
        )
    return _pass("vocabulary", "VOCABULARY_OK")


def _structural_validation(query: str, ontology_context: dict[str, object]) -> ValidationStageResult:
    del ontology_context
    body = _query_body(query)
    upper_body = body.upper()
    if not upper_body.startswith(("SELECT", "ASK", "CONSTRUCT", "DESCRIBE")):
        return _fail(
            "structural",
            "QUERY_FORM_INVALID",
            "Generated query must start with SELECT, ASK, CONSTRUCT, or DESCRIBE",
        )
    if "WHERE" not in upper_body:
        return _fail("structural", "WHERE_MISSING", "Generated query must contain a WHERE clause")
    if "{" not in body or "}" not in body:
        return _fail(
            "structural",
            "WHERE_PATTERN_MISSING",
            "Generated query must contain a graph pattern enclosed in braces",
        )

    select_vars = _select_variables(body)
    where_vars = _where_variables(body)
    missing_vars = sorted(var for var in select_vars if var not in where_vars)
    if missing_vars:
        return _fail(
            "structural",
            "SELECT_VARIABLE_NOT_BOUND",
            f"SELECT variables are not bound in WHERE: {', '.join(missing_vars)}",
        )

    if _has_aggregation(body) and "GROUP BY" not in upper_body:
        return _fail(
            "structural",
            "GROUP_BY_MISSING",
            "GROUP BY must be present when aggregation functions are used",
        )

    return _pass("structural", "STRUCTURE_OK")


def _normalized_query(query: str, ontology_context: dict[str, object]) -> str:
    query_body = (query or "").strip()
    if not query_body:
        return ""

    declarations = []
    declared = _declared_prefixes(query_body)
    for prefix, namespace in _ontology_prefixes(ontology_context).items():
        if prefix in declared:
            continue
        declarations.append(f"PREFIX : <{namespace}>" if prefix == "" else f"PREFIX {prefix}: <{namespace}>")

    if not declarations:
        return query_body
    return "\n".join([*declarations, "", query_body])


def _query_body(query: str) -> str:
    lines = query.splitlines()
    body_lines = []
    skipping_prefix_block = True

    for line in lines:
        stripped = line.strip()
        if skipping_prefix_block and stripped.upper().startswith("PREFIX "):
            continue
        if skipping_prefix_block and not stripped:
            continue
        skipping_prefix_block = False
        body_lines.append(line)

    return "\n".join(body_lines).strip()


def _declared_prefixes(query: str) -> dict[str, str]:
    prefixes: dict[str, str] = {}
    for match in re.finditer(r"(?im)^\s*PREFIX\s+([A-Za-z_][\w-]*|):\s*<([^>]+)>", query):
        prefixes[match.group(1)] = match.group(2)
    return prefixes


def _ontology_prefixes(ontology_context: dict[str, object]) -> dict[str, str]:
    prefixes = ontology_context.get("prefixes", [])
    if not isinstance(prefixes, list):
        return {}

    result: dict[str, str] = {}
    for item in prefixes:
        if not isinstance(item, dict):
            continue
        prefix = item.get("prefix")
        namespace = item.get("namespace")
        if isinstance(prefix, str) and isinstance(namespace, str):
            result["" if prefix == ":" else prefix] = namespace
    return result


def _used_prefixes(query: str) -> set[str]:
    query_without_prefixes = "\n".join(
        line for line in query.splitlines() if not line.strip().upper().startswith("PREFIX ")
    )
    used = set()
    for match in re.finditer(r"(?<![A-Za-z0-9_/-])([A-Za-z_][\w-]*):([A-Za-z_][\w.-]*)", query_without_prefixes):
        prefix = match.group(1)
        if prefix.lower() in {"http", "https"}:
            continue
        used.add(prefix)
    return used


def _ontology_vocabulary(ontology_context: dict[str, object]) -> tuple[set[str], set[str]]:
    class_uris = _uris_from_entries(ontology_context.get("classes", []))
    property_uris = _uris_from_entries(ontology_context.get("object_properties", []))
    property_uris.update(_uris_from_entries(ontology_context.get("datatype_properties", [])))
    return class_uris, property_uris


def _uris_from_entries(entries: object) -> set[str]:
    if not isinstance(entries, list):
        return set()
    uris = set()
    for item in entries:
        if isinstance(item, dict) and isinstance(item.get("uri"), str):
            uris.add(str(item["uri"]))
    return uris


def _predicate_uris(query: str, prefix_map: dict[str, str]) -> set[str]:
    body = _where_body(query)
    uris: set[str] = set()
    for statement in re.split(r"\s+\.\s+", body):
        tokens = statement.strip().split()
        if len(tokens) < 3:
            continue
        predicate = tokens[1]
        expanded = _expand_term(predicate, prefix_map)
        if expanded:
            uris.add(expanded)
    return uris


def _rdf_type_object_uris(query: str, prefix_map: dict[str, str]) -> set[str]:
    body = _where_body(query)
    uris: set[str] = set()
    for statement in re.split(r"\s+\.\s+", body):
        tokens = statement.strip().split()
        if len(tokens) < 3:
            continue
        predicate = _expand_term(tokens[1], prefix_map)
        if not _is_rdf_type_predicate(predicate or tokens[1]):
            continue
        expanded = _expand_term(tokens[2], prefix_map)
        if expanded:
            uris.add(expanded)
    return uris


def _where_body(query: str) -> str:
    match = re.search(r"\{(.*)\}", query, re.DOTALL)
    return match.group(1) if match else ""


def _expand_term(term: str, prefix_map: dict[str, str]) -> str | None:
    cleaned = term.strip().strip(";,.")
    if cleaned == "a":
        return "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    if cleaned.startswith("<") and cleaned.endswith(">"):
        return cleaned[1:-1]
    prefixed = re.match(r"^([A-Za-z_][\w-]*|):([A-Za-z_][\w.-]*)$", cleaned)
    if not prefixed:
        return None
    prefix, local = prefixed.groups()
    namespace = prefix_map.get(prefix)
    return f"{namespace}{local}" if namespace else None


def _is_builtin_predicate(uri: str) -> bool:
    return uri in {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://www.w3.org/2000/01/rdf-schema#label",
        "http://www.w3.org/2000/01/rdf-schema#comment",
    }


def _is_rdf_type_predicate(value: str) -> bool:
    return value in {
        "a",
        "rdf:type",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
    }


def _select_variables(query_body: str) -> set[str]:
    match = re.search(r"(?is)\bSELECT\b(.*?)\bWHERE\b", query_body)
    if not match:
        return set()
    select_text = re.sub(r"\([^)]*\)", " ", match.group(1))
    if "*" in select_text:
        return set()
    return {var.lstrip("?") for var in re.findall(r"\?[A-Za-z_][\w-]*", select_text)}


def _where_variables(query_body: str) -> set[str]:
    return {var.lstrip("?") for var in re.findall(r"\?[A-Za-z_][\w-]*", _where_body(query_body))}


def _has_aggregation(query_body: str) -> bool:
    return bool(re.search(r"(?i)\b(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|SAMPLE)\s*\(", query_body))


def _pass(stage: str, code: str) -> ValidationStageResult:
    return ValidationStageResult(stage=stage, passed=True, code=code)


def _fail(stage: str, code: str, message: str) -> ValidationStageResult:
    return ValidationStageResult(stage=stage, passed=False, code=code, message=message)
