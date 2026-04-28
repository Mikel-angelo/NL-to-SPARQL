"""Render runtime LLM prompts from retrieved ontology context."""

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
    """Render the SPARQL generation prompt from dynamic ontology/query inputs."""
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
            "Use full URIs in angle brackets for classes and properties. "
            "Do not use prefixed names or invented prefixes. "
            "Do not include explanations, markdown fences, or extra text."
        ),
        user_question=question.strip(),
    )


def render_correction_prompt(
    *,
    original_question: str,
    failed_query: str,
    validation_errors: list[str],
) -> str:
    """Render feedback for one failed SPARQL correction attempt."""
    template = _template_environment().get_template("query_correction_prompt.j2")
    return template.render(
        original_question=original_question.strip(),
        failed_query=failed_query.strip(),
        validation_errors=validation_errors,
    )


def prefix_declarations(ontology_context: dict[str, object]) -> list[str]:
    """Build PREFIX declarations from ontology context prefixes."""
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
