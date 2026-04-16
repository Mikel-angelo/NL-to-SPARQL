"""Attempt one correction pass when generated SPARQL fails validation."""


class QueryCorrectionService:
    """Repair a narrow set of deterministic query-shape issues for runtime v1."""

    def correct(self, query: str, errors: list[str]) -> str | None:
        """Return a corrected query when a safe deterministic fix is available."""
        del errors

        normalized_query = (query or "").strip()
        if not normalized_query:
            return None

        if normalized_query.startswith("{") and normalized_query.endswith("}"):
            return f"SELECT * WHERE {normalized_query} LIMIT 25"

        upper_query = normalized_query.upper()
        if upper_query.startswith("SELECT") and "WHERE" not in upper_query:
            open_brace = normalized_query.find("{")
            close_brace = normalized_query.rfind("}")
            if open_brace != -1 and close_brace != -1 and open_brace < close_brace:
                head = normalized_query[:open_brace].rstrip()
                body = normalized_query[open_brace:]
                return f"{head} WHERE {body}"

        return None
