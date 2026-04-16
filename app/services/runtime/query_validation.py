"""Validate generated SPARQL before execution."""

from dataclasses import dataclass


@dataclass(frozen=True)
class QueryValidationResult:
    """Validation outcome for one candidate SPARQL query."""

    is_valid: bool
    errors: list[str]
    normalized_query: str


class QueryValidationService:
    """Perform lightweight syntactic validation for runtime v1."""

    _SUPPORTED_QUERY_FORMS = ("SELECT", "ASK", "CONSTRUCT", "DESCRIBE")

    def validate(
        self,
        query: str,
        ontology_context: dict[str, object] | None = None,
    ) -> QueryValidationResult:
        normalized_query = self._normalized_query(query, ontology_context or {})
        errors: list[str] = []

        if not normalized_query:
            errors.append("Generated query is empty")
        else:
            query_body = self._query_body(normalized_query)
            upper_query_body = query_body.upper()
            if not upper_query_body.startswith(self._SUPPORTED_QUERY_FORMS):
                errors.append(
                    "Generated query must start with SELECT, ASK, CONSTRUCT, or DESCRIBE"
                )
            if "WHERE" not in upper_query_body:
                errors.append("Generated query must contain a WHERE clause")
            if "{" not in query_body or "}" not in query_body:
                errors.append("Generated query must contain a graph pattern enclosed in braces")

        return QueryValidationResult(
            is_valid=not errors,
            errors=errors,
            normalized_query=normalized_query,
        )

    @staticmethod
    def _normalized_query(query: str, ontology_context: dict[str, object]) -> str:
        query_body = (query or "").strip()
        if not query_body:
            return ""

        prefix_declarations = QueryValidationService._prefix_declarations(ontology_context)
        if not prefix_declarations:
            return query_body

        query_lines = query_body.splitlines()
        declared_prefixes = {
            line.strip().split()[1]
            for line in query_lines
            if line.strip().upper().startswith("PREFIX ")
            and len(line.strip().split()) >= 2
        }

        missing_prefixes = []
        for declaration in prefix_declarations:
            parts = declaration.split()
            if len(parts) < 2:
                continue
            prefix_token = parts[1]
            if prefix_token not in declared_prefixes:
                missing_prefixes.append(declaration)

        if not missing_prefixes:
            return query_body

        return "\n".join([*missing_prefixes, "", query_body])

    @staticmethod
    def _prefix_declarations(ontology_context: dict[str, object]) -> list[str]:
        prefixes = ontology_context.get("prefixes", [])
        if not isinstance(prefixes, list):
            return []

        declarations: list[str] = []
        for item in prefixes:
            if not isinstance(item, dict):
                continue
            prefix = item.get("prefix")
            namespace = item.get("namespace")
            if not isinstance(prefix, str) or not isinstance(namespace, str):
                continue
            if prefix == ":":
                declarations.append(f"PREFIX : <{namespace}>")
            else:
                declarations.append(f"PREFIX {prefix}: <{namespace}>")
        return declarations

    @staticmethod
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
