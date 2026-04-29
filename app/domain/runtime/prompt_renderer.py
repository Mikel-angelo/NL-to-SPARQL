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
        system_role=(
            "You are an expert SPARQL query generator. "
            "Use only the provided ontology context and URIs. "
            "Do not invent classes, properties, or namespaces."
        ),
        ontology_name=metadata.get("ontology_name") if isinstance(metadata.get("ontology_name"), str) else None,
        dataset_name=metadata.get("dataset_name") if isinstance(metadata.get("dataset_name"), str) else None,
        retrieved_context=[{"rank": item.rank, "text": item.text} for item in retrieved_context],
        prefix_declarations=prefix_declarations(ontology_context),
        few_shot_examples=[],
        output_format_instructions=(
            "Return only one valid SPARQL query. "
            "Use either full URIs in angle brackets or the provided prefix declarations. "
            "Only prefixes listed under Auto-Generated Prefix Declarations are allowed. "
            "Ontology and dataset names are labels, not SPARQL prefixes. "
            "Use the ':' prefix for terms in the default ontology namespace when it is listed. "
            "When returning ontology entities, prefer human-readable labels when label predicates are available. "
            "Do not invent prefixes, classes, properties, or namespaces. "
            "Do not include explanations, markdown fences, or extra text."
        ),
        user_question=question.strip(),
    )


def render_correction_prompt(
    *,
    original_question: str,
    failed_query: str,
    validation_errors: list[str],
    ontology_context: dict[str, object] | None = None,
) -> str:
    """Render the correction prompt for one failed runtime attempt.

    The supplied errors may come from formal validation or endpoint execution;
    both are presented as feedback for the next candidate query.
    """
    template = _template_environment().get_template("query_correction_prompt.j2")
    return template.render(
        original_question=original_question.strip(),
        failed_query=failed_query.strip(),
        validation_errors=validation_errors,
        prefix_declarations=prefix_declarations(ontology_context or {}),
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


def _template_environment() -> Environment:
    template_dir = Path(__file__).resolve().parent / "templates"
    return Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=StrictUndefined,
    )
