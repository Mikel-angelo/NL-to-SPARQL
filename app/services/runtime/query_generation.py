"""Generate SPARQL from a question and retrieved ontology context."""

from pathlib import Path
import json
import re

import httpx
from fastapi import HTTPException, status
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from app.core.config import settings


class QueryGenerationService:
    """Build an LLM-ready prompt from runtime artifacts and call Ollama."""

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
        self._ollama_url = settings.ollama_url
        self._ollama_model = settings.ollama_model
        self._timeout = settings.llm_timeout_seconds
        self._temperature = settings.llm_temperature
        self._num_ctx = settings.llm_num_ctx

    async def generate(
        self,
        question: str,
        retrieved_context: list[dict[str, object]],
        metadata: dict[str, object],
    ) -> str:
        """Render the generation prompt, call Ollama, and return cleaned SPARQL text."""
        prompt = self.render_prompt(
            question=question,
            retrieved_context=retrieved_context,
            metadata=metadata,
        )
        generated_text = await self.generate_from_prompt(prompt)
        normalized_query = self._normalize_generated_query(generated_text)
        if not normalized_query:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="The LLM returned an empty query",
            )
        return normalized_query

    async def generate_from_prompt(self, prompt: str) -> str:
        """Call Ollama for one already-rendered prompt and return cleaned query text."""
        generated_text = await self._generate_text(prompt)
        normalized_query = self._normalize_generated_query(generated_text)
        if not normalized_query:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="The LLM returned an empty query",
            )
        return normalized_query

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
            triple_count=metadata.get("triple_count"),
            retrieved_context=self._normalized_retrieved_context(retrieved_context),
            prefix_declarations=self._prefix_declarations(ontology_context),
            few_shot_examples=self._few_shot_examples(few_shot_examples),
            output_format_instructions=self._output_format_instructions(),
            user_question=question.strip(),
        )

    async def _generate_text(self, prompt: str) -> str:
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

    @staticmethod
    def _normalize_generated_query(generated_text: str) -> str:
        text = generated_text.strip()
        if text.startswith("```"):
            fenced_match = re.match(r"^```[A-Za-z0-9_-]*\s*(.*?)```$", text, re.DOTALL)
            if fenced_match:
                text = fenced_match.group(1).strip()
        return text

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
                    "class_name": item.get("class_name"),
                    "class_uri": item.get("class_uri"),
                    "text": item.get("text"),
                    "metadata": item.get("metadata"),
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
            declarations.append(QueryGenerationService._prefix_declaration(prefix, namespace))
        return declarations

    @staticmethod
    def _prefix_declaration(prefix: str, namespace: str) -> str:
        if prefix == ":":
            return f"PREFIX : <{namespace}>"
        return f"PREFIX {prefix}: <{namespace}>"
