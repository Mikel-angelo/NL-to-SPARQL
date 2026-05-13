"""Render runtime LLM prompts from package and retrieval context.

This module is the only runtime layer that knows about the Jinja templates. It
turns package metadata, retrieved RAG chunks, ontology prefixes, failed queries,
and validation/execution feedback into prompt strings. It does not call the LLM
or decide when prompts should be rendered.
"""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from app.domain.rag import RetrievedChunk


SYSTEM_ROLE = (
    "You are an expert SPARQL query generator. "
    "Use only the provided ontology context and URIs. "
    "Do not invent classes, properties, or namespaces."
)

CORRECTION_SYSTEM_ROLE = (
    "You are an expert SPARQL query generator. "
    "Your previous query failed validation. "
    "Read the error messages carefully — they identify the exact problem. "
    "Read the ontology chunks carefully — they contain the correct class and property names. "
    "Fix the query using only URIs that exist in the provided ontology context."
)

PROMPT_RULES = """Prefix Usage Rules:
- Use only the prefix declarations listed above.
- Do not use the ontology label or dataset label as a prefix.
- If a default prefix declaration is listed as `PREFIX : <...>`, use terms such as `:ClassName` for that namespace.
- Unknown prefixes will fail validation.

Entity Matching Rules:
- Never construct individual/instance URIs directly (e.g., :CAMPUS_VESTA, :UCLouvain-CTMA).
- Instance URIs are opaque identifiers that cannot be guessed from labels.
- Always find instances by class and label using this pattern:
  ?entity rdf:type :ClassName ; rdfs:label ?label .
  FILTER(CONTAINS(LCASE(STR(?label)), "search term"))
- Use LCASE and CONTAINS for partial, case-insensitive matching.
- When the question names a specific entity, extract the key words for the FILTER.
  Example: "CAMPUS VESTA" → FILTER(CONTAINS(LCASE(STR(?label)), "campus vesta"))
- When no specific entity is named, omit the FILTER to match all instances.

Result Shape Rules:
- If the answer includes an ontology entity/resource and `rdfs:label` is available, return the label variable instead of the entity URI.
- Use `rdfs:label` for labels when the `rdfs:` prefix is listed above.
- Use `skos:prefLabel` as another label option only when the `skos:` prefix is listed above.
- For aggregate queries grouped by an ontology entity/resource, do not return only the grouping URI. Join the resource to its label and return the label variable as the displayed answer. Group by both the resource and the label when needed.
- If a label might not exist, use `OPTIONAL` and return both the entity URI and the label variable, or use `COALESCE` to expose the label when present and the URI as fallback.
- Return an entity URI only when the question explicitly asks for URIs or no label predicate is available.
- Do not invent label properties or label prefixes.

Result Shape Example:
For questions that count or group resources, expose the resource label:
SELECT ?entityLabel (COUNT(?item) AS ?count)
WHERE {
  ?entity :someProperty ?item .
  ?entity rdfs:label ?entityLabel .
}
GROUP BY ?entityLabel

Strict Constraints:
- Only use class and property names that appear in the Relevant Ontology Chunks above.
- If the exact property name is not visible in the chunks, re-read them carefully before writing the query. Do not guess or invent property names.
- Every variable in SELECT must appear in the WHERE clause.
- Do not use OPTIONAL unless the question explicitly implies some data may be missing.
"""

OUTPUT_FORMAT_INSTRUCTIONS = """Output Format Instructions:
- Return only one valid SPARQL query.
- Use either full URIs in angle brackets or the provided prefix declarations.
- Only prefixes listed under Auto-Generated Prefix Declarations are allowed.
- Ontology and dataset names are labels, not SPARQL prefixes.
- Use the ':' prefix for terms in the default ontology namespace when it is listed.
- Do not invent prefixes, classes, properties, or namespaces.
- Do not include explanations, markdown fences, or extra text."""

CORRECTION_OUTPUT_FORMAT_INSTRUCTIONS = (
    "Return only a corrected SPARQL query. "
    "Use either full URIs in angle brackets or the available prefix declarations. "
    "Do not invent prefixes, classes, properties, or namespaces. "
    "Do not include explanations, markdown fences, or extra text."
)


def render_query_generation_prompt(
    *,
    question: str,
    retrieved_context: list[RetrievedChunk],
    metadata: dict[str, object],
    ontology_context: dict[str, object],
) -> str:
    """Render the first-query generation prompt.

    The prompt contains the user question, retrieved ontology chunk text,
    available prefix declarations, and output constraints that ask the model to
    return a single SPARQL query without explanations.
    """
    template = _template_environment().get_template("query_generation_prompt.j2")
    return template.render(
        system_role=SYSTEM_ROLE,
        ontology_name=metadata.get("ontology_name") if isinstance(metadata.get("ontology_name"), str) else None,
        dataset_name=metadata.get("dataset_name") if isinstance(metadata.get("dataset_name"), str) else None,
        retrieved_context=[{"rank": item.rank, "text": item.text} for item in retrieved_context],
        prefix_declarations=prefix_declarations(ontology_context),
        prompt_rules=PROMPT_RULES,
        few_shot_examples=[],
        output_format_instructions=OUTPUT_FORMAT_INSTRUCTIONS,
        user_question=question.strip(),
    )


def render_correction_prompt(
    *,
    original_question: str,
    failed_query: str,
    validation_errors: list[str],
    retrieved_context: list[RetrievedChunk] | list[dict[str, object]] | None = None,
    ontology_context: dict[str, object] | None = None,
) -> str:
    """Render the correction prompt for one failed runtime attempt.

    The supplied errors may come from formal validation or endpoint execution;
    both are presented as feedback for the next candidate query.
    """
    template = _template_environment().get_template("query_correction_prompt.j2")
    return template.render(
        system_role=CORRECTION_SYSTEM_ROLE,
        original_question=original_question.strip(),
        failed_query=failed_query.strip(),
        validation_errors=validation_errors,
        retrieved_context=_retrieved_context_payload(retrieved_context or []),
        prefix_declarations=prefix_declarations(ontology_context or {}),
        prompt_rules=PROMPT_RULES,
        output_format_instructions=CORRECTION_OUTPUT_FORMAT_INSTRUCTIONS,
    )


def prefix_declarations(ontology_context: dict[str, object]) -> list[str]:
    """Build SPARQL `PREFIX` declarations from ontology context prefixes.

    `ontology_context.json` stores the default prefix as `":"`; this function
    converts it back to valid SPARQL declaration syntax.
    """
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


def _retrieved_context_payload(
    retrieved_context: list[RetrievedChunk] | list[dict[str, object]],
) -> list[dict[str, object]]:
    payload = []
    for index, item in enumerate(retrieved_context, 1):
        if isinstance(item, RetrievedChunk):
            payload.append({"rank": item.rank, "text": item.text})
        elif isinstance(item, dict):
            payload.append(
                {
                    "rank": item.get("rank", index),
                    "text": item.get("text", ""),
                }
            )
    return payload


def _template_environment() -> Environment:
    template_dir = Path(__file__).resolve().parent / "templates"
    return Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=StrictUndefined,
    )
