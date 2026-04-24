"""Build runtime prompts and call the configured LLM."""

from pathlib import Path
import json
import re
import httpx
from fastapi import HTTPException, status
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from app.core.config import settings


class PromptBuilder:
    """Render the runtime SPARQL prompt from stored ontology artifacts."""

    def __init__(
        self,
        storage_dir: Path | None = None,
        template_dir: Path | None = None,
    ) -> None:
        self._storage_dir = storage_dir or Path(settings.storage_path)
        self._current_dir = self._storage_dir / "current"
        self._ontology_context_path = self._current_dir / "ontology_context.json"
        self._template_dir = template_dir or Path(__file__).resolve().parent / "templates"
        self._template_environment = Environment(
            loader=FileSystemLoader(str(self._template_dir)),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
            undefined=StrictUndefined,
        )
        self._template_name = "query_generation_prompt.j2"

    def render_prompt(
        self,
        question: str,
        retrieved_context: list[dict[str, object]],
        metadata: dict[str, object],
        few_shot_examples: list[dict[str, str]] | None = None,
    ) -> str:
        """Render the runtime prompt with ontology context and auto-generated prefixes."""
        ontology_context = self._load_ontology_context()
        template = self._template_environment.get_template(self._template_name)
        return template.render(
            system_role=self._system_role_text(),
            ontology_name=metadata.get("ontology_name") if isinstance(metadata.get("ontology_name"), str) else None,
            dataset_name=metadata.get("dataset_name") if isinstance(metadata.get("dataset_name"), str) else None,
            retrieved_context=self._normalized_retrieved_context(retrieved_context),
            prefix_declarations=self._prefix_declarations(ontology_context),
            few_shot_examples=self._few_shot_examples(few_shot_examples),
            output_format_instructions=self._output_format_instructions(),
            user_question=question.strip(),
        )

    @staticmethod
    def _system_role_text() -> str:
        return (
            "You are an expert SPARQL query generator. "
            "Use only the provided ontology context and URIs. "
            "Do not invent classes, properties, or namespaces."
        )

    @staticmethod
    def _output_format_instructions() -> str:
        return (
            "Return only one valid SPARQL query. "
            "Use full URIs in angle brackets for classes and properties. "
            "Do not use prefixed names such as :ActorType, rdf:type, rdfs:label, or invented prefixes. "
            "Do not include explanations, markdown fences, or extra text."
        )

    def _load_ontology_context(self) -> dict[str, object]:
        if not self._ontology_context_path.exists():
            return {}

        try:
            ontology_context = json.loads(self._ontology_context_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

        if not isinstance(ontology_context, dict):
            return {}
        return ontology_context

    @staticmethod
    def _normalized_retrieved_context(
        retrieved_context: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        normalized: list[dict[str, object]] = []
        for item in retrieved_context:
            normalized.append(
                {
                    "rank": item.get("rank"),
                    "text": item.get("text"),
                }
            )
        return normalized

    @staticmethod
    def _few_shot_examples(
        few_shot_examples: list[dict[str, str]] | None,
    ) -> list[dict[str, str]]:
        del few_shot_examples
        return []

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
            declarations.append(PromptBuilder._prefix_declaration(prefix, namespace))
        return declarations

    @staticmethod
    def _prefix_declaration(prefix: str, namespace: str) -> str:
        if prefix == ":":
            return f"PREFIX : <{namespace}>"
        return f"PREFIX {prefix}: <{namespace}>"


class LLMClient:
    """Call the configured Ollama endpoint and return raw generated text."""

    def __init__(self) -> None:
        self._ollama_url = settings.ollama_url
        self._ollama_model = settings.ollama_model
        self._timeout = settings.llm_timeout_seconds
        self._temperature = settings.llm_temperature
        self._num_ctx = settings.llm_num_ctx

    async def generate_text(self, prompt: str) -> str:
        payload = {
            "model": self._ollama_model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": self._temperature,
                "num_ctx": self._num_ctx,
            },
        }

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(self._ollama_url, json=payload)
                response.raise_for_status()
        except httpx.TimeoutException as exc:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Timed out while generating a SPARQL query",
            ) from exc
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"LLM generation failed ({exc.response.status_code}): {exc.response.text}",
            ) from exc
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"LLM generation failed: {exc}",
            ) from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="The LLM returned a non-JSON response",
            ) from exc

        generated_text = payload.get("response")
        if not isinstance(generated_text, str):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="The LLM response is missing generated text",
            )
        return generated_text


def normalize_generated_query(generated_text: str) -> str:
    """Convert raw LLM output into plain SPARQL text."""
    text = generated_text.strip()
    if text.startswith("```"):
        fenced_match = re.match(r"^```[A-Za-z0-9_-]*\s*(.*?)```$", text, re.DOTALL)
        if fenced_match:
            text = fenced_match.group(1).strip()
    if not text:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The LLM returned an empty query",
        )
    return text
