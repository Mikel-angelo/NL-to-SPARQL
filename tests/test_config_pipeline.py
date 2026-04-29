"""Tests for the ontology-package onboarding and runtime pipeline."""

from __future__ import annotations

import tempfile
import unittest
import json
from importlib import import_module
from pathlib import Path

import numpy as np
from rdflib import Graph

from app.domain import package as package_module
from app.domain.ontology.graph_preparation import prepare_final_graph
from app.domain.ontology.ontology_context import build_ontology_context
from app.domain.ontology.package_writer import OntologyPackageArtifacts, write_ontology_package
from app.domain.ontology.source_loader import LoadedOntologySource, load_ontology_file
from app.domain.package import get_active_package
from app.domain.rag import build_index, retrieve_text_chunks
from app.domain.rag.chunking import build_chunks
from app.domain.rag.retrieve_context import RetrievedChunk
from app.domain.runtime import run_query_attempts, run_query_pipeline
from app.domain.runtime import query_correction as query_correction_module
from app.domain.runtime import query_generation as query_generation_module
from app.domain.runtime import sparql_execution as sparql_execution_module
from app.domain.runtime.prompt_renderer import render_correction_prompt, render_query_generation_prompt
from app.domain.runtime.validation import validate_query

index_module = import_module("app.domain.rag.build_index")


ONTOLOGY_TTL = """@prefix ex: <http://example.com/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:Person a owl:Class ; rdfs:label "Person" .
ex:Place a owl:Class ; rdfs:label "Place" .
ex:Employee a owl:Class ; rdfs:label "Employee" ; rdfs:subClassOf ex:Person .
ex:worksAt a owl:ObjectProperty ; rdfs:domain ex:Person ; rdfs:range ex:Place .
ex:hasName a owl:DatatypeProperty ; rdfs:domain ex:Person ; rdfs:range rdfs:Literal .
ex:alice a ex:Person .
"""


def deterministic_embeddings(texts: list[str]) -> np.ndarray:
    """Return stable small vectors without loading a sentence-transformer model."""
    vectors = []
    for index, _ in enumerate(texts):
        if index == 0:
            vectors.append([1.0, 0.0])
        else:
            vectors.append([0.0, 1.0])
    return np.asarray(vectors, dtype="float32")


class PackagePipelineTests(unittest.IsolatedAsyncioTestCase):
    """Validate the new ontology-package flow without external network calls."""

    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tempdir.name)
        self.package_dir = self.root / "ontology_packages" / "example"
        self.ontology_path = self.root / "ontology.ttl"
        self.ontology_path.write_text(ONTOLOGY_TTL, encoding="utf-8")

        self._patch(index_module, "embed_texts", deterministic_embeddings)
        self._patch(query_generation_module, "generate_text", self._fake_generate_text)
        self._patch(query_correction_module, "generate_text", self._fake_generate_text)
        self._patch(sparql_execution_module, "execute_sparql_query", self._fake_execute_query)

    def tearDown(self) -> None:
        self._tempdir.cleanup()

    def _patch(self, module: object, name: str, replacement: object) -> None:
        original = getattr(module, name)
        setattr(module, name, replacement)
        self.addCleanup(setattr, module, name, original)

    async def _fake_generate_text(self, prompt: str, *, model: str, llm_api_url: str) -> str:
        del prompt, model, llm_api_url
        return "SELECT * WHERE { ?s ?p ?o } LIMIT 5"

    async def _fake_execute_query(self, endpoint: str, query: str) -> dict[str, object]:
        return {
            "endpoint": endpoint,
            "query": query,
            "results": {"bindings": []},
        }

    async def _prepare_file_package(
        self,
        *,
        dataset_name: str = "example-dataset",
        query_endpoint: str = "http://example.test/dataset/query",
    ) -> OntologyPackageArtifacts:
        source = await load_ontology_file(str(self.ontology_path))
        final_graph = await prepare_final_graph(source.graph)
        ontology_context = build_ontology_context(
            final_graph.graph,
            ontology_name="ontology",
            source_filename=self.ontology_path.name,
        )
        return write_ontology_package(
            package_dir=self.package_dir,
            source=source,
            final_graph=final_graph,
            ontology_name="ontology",
            ontology_context=ontology_context,
            dataset_name=dataset_name,
            query_endpoint=query_endpoint,
            default_model=None,
            chunking="class_based",
        )

    async def _prepare_endpoint_package(
        self,
        *,
        endpoint: str = "http://example.test/sparql",
    ) -> OntologyPackageArtifacts:
        graph = Graph()
        graph.parse(data=ONTOLOGY_TTL, format="turtle")
        source = LoadedOntologySource(
            graph=graph,
            source_mode="sparql_endpoint",
            source_name=endpoint,
            source_path=None,
            content=None,
            suffix=None,
            query_endpoint=endpoint,
        )
        final_graph = await prepare_final_graph(source.graph, resolve_missing_schemas=False)
        ontology_context = build_ontology_context(
            final_graph.graph,
            ontology_name="sparql",
            source_filename=endpoint,
        )
        return write_ontology_package(
            package_dir=self.package_dir,
            source=source,
            final_graph=final_graph,
            ontology_name="sparql",
            ontology_context=ontology_context,
            dataset_name=None,
            query_endpoint=endpoint,
            default_model=None,
            chunking="class_based",
        )

    async def test_file_extraction_and_runtime_artifacts_create_expected_layout(self) -> None:
        artifacts = await self._prepare_file_package()
        artifact_result = build_index(self.package_dir, chunking="class_based")

        self.assertEqual(artifacts.metadata["source_mode"], "file")
        self.assertTrue((self.package_dir / "ontology").exists())
        self.assertTrue((self.package_dir / "metadata.json").exists())
        self.assertTrue((self.package_dir / "ontology_context.json").exists())
        self.assertTrue((self.package_dir / "settings.json").exists())
        self.assertTrue(artifact_result.chunks_path.exists())
        self.assertTrue(artifact_result.index_path.exists())
        self.assertEqual(artifact_result.chunks_path.parent.name, "chunks")
        self.assertEqual(artifact_result.index_path.parent.name, "chunks")

    async def test_chunking_strategies_build_class_property_and_composite_chunks(self) -> None:
        artifacts = await self._prepare_file_package()

        class_chunks = build_chunks(artifacts.ontology_context, "class_based")
        property_chunks = build_chunks(artifacts.ontology_context, "property_based")
        composite_chunks = build_chunks(artifacts.ontology_context, "composite")

        self.assertTrue(any(chunk["chunk_type"] == "class" for chunk in class_chunks))
        self.assertTrue(any(chunk.get("property_name") == "worksAt" for chunk in property_chunks))
        self.assertTrue(any("Domain Classes:" in str(chunk.get("text")) for chunk in property_chunks))
        self.assertTrue(any("Place" in str(chunk.get("text")) for chunk in property_chunks))
        self.assertTrue(any(chunk.get("class_name") == "Person" for chunk in composite_chunks))
        self.assertTrue(any("Child Classes:" in str(chunk.get("text")) for chunk in composite_chunks))
        self.assertTrue(any("Employee" in str(chunk.get("text")) for chunk in composite_chunks))

    async def test_endpoint_extraction_creates_config_without_source_file(self) -> None:
        artifacts = await self._prepare_endpoint_package()
        build_index(self.package_dir, chunking="class_based")

        self.assertEqual(artifacts.metadata["source_mode"], "sparql_endpoint")
        self.assertFalse(any(path.name.startswith("source.") for path in (self.package_dir / "ontology").glob("*")))
        self.assertEqual(artifacts.settings["query_endpoint"], "http://example.test/sparql")

    async def test_run_query_pipeline_uses_config_artifacts(self) -> None:
        await self._prepare_file_package()
        build_index(self.package_dir, chunking="class_based")

        result = await run_query_pipeline(
            "Which places exist?",
            self.package_dir,
            model="stub-model",
        )

        self.assertEqual(result.status, "completed")
        self.assertIn("SELECT", result.generated_sparql or "")
        self.assertTrue(Path(result.trace_path).exists())
        self.assertTrue(Path(result.readable_trace_path).exists())
        readable_trace = Path(result.readable_trace_path).read_text(encoding="utf-8")
        self.assertIn("GENERATION PROMPT", readable_trace)
        self.assertIn("INITIAL GENERATED QUERY", readable_trace)
        self.assertGreaterEqual(len(result.retrieved_context), 1)

    async def test_rag_retrieve_text_chunks_returns_plain_text(self) -> None:
        await self._prepare_file_package()
        build_index(self.package_dir, chunking="class_based")

        chunks = retrieve_text_chunks(self.package_dir, "Which places exist?", k=2)

        self.assertGreaterEqual(len(chunks), 1)
        self.assertTrue(all(isinstance(chunk, str) for chunk in chunks))

    def test_prompt_renderer_fills_dynamic_sections(self) -> None:
        prompt = render_query_generation_prompt(
            question="Which places exist?",
            retrieved_context=[
                RetrievedChunk(
                    rank=1,
                    score=0.1,
                    class_name="Place",
                    class_uri="http://example.com/Place",
                    text="Class: Place",
                    metadata=None,
                )
            ],
            metadata={"ontology_name": "example", "dataset_name": "example-dataset"},
            ontology_context={
                "prefixes": [
                    {"prefix": "ex", "namespace": "http://example.com/"},
                ]
            },
        )

        self.assertIn("Class: Place", prompt)
        self.assertIn("PREFIX ex: <http://example.com/>", prompt)
        self.assertIn("provided prefix declarations", prompt)
        self.assertIn("Ontology label, not a SPARQL prefix", prompt)
        self.assertIn("Do not use the ontology label or dataset label as a prefix", prompt)
        self.assertIn("Which places exist?", prompt)
        self.assertNotIn(":ActorType", prompt)

    def test_correction_prompt_renderer_uses_feedback_template(self) -> None:
        prompt = render_correction_prompt(
            original_question="Which places exist?",
            failed_query="SELECT ?x WHERE { ?s ?p ?o }",
            validation_errors=["SELECT variables are not bound in WHERE: x"],
            ontology_context={
                "prefixes": [
                    {"prefix": "ex", "namespace": "http://example.com/"},
                ]
            },
        )

        self.assertIn("Original question:", prompt)
        self.assertIn("Failed query:", prompt)
        self.assertIn("SELECT variables are not bound in WHERE: x", prompt)
        self.assertIn("PREFIX ex: <http://example.com/>", prompt)
        self.assertIn("Do not use the ontology label or dataset label as a prefix", prompt)
        self.assertIn("Do not invent prefixes", prompt)

    def test_validation_returns_formal_stage_results(self) -> None:
        result = validate_query(
            "SELECT ?place WHERE { ?person <http://example.com/worksAt> ?place . }",
            ontology_context={
                "object_properties": [{"uri": "http://example.com/worksAt"}],
                "datatype_properties": [],
                "classes": [],
                "prefixes": [],
            },
        )

        self.assertTrue(result.is_valid)
        self.assertEqual(
            [stage.stage for stage in result.stages],
            ["syntactic", "prefix", "vocabulary", "structural"],
        )
        self.assertTrue(all(stage.passed for stage in result.stages))

    def test_validation_allows_known_prefixes_and_rejects_unknown_prefixes(self) -> None:
        ontology_context = {
            "object_properties": [{"uri": "http://example.com/worksAt"}],
            "datatype_properties": [],
            "classes": [],
            "prefixes": [{"prefix": "ex", "namespace": "http://example.com/"}],
        }

        known = validate_query(
            "SELECT ?place WHERE { ?person ex:worksAt ?place . }",
            ontology_context=ontology_context,
        )
        unknown = validate_query(
            "SELECT ?place WHERE { ?person bad:worksAt ?place . }",
            ontology_context=ontology_context,
        )

        self.assertTrue(known.is_valid)
        self.assertIn("PREFIX ex: <http://example.com/>", known.normalized_query)
        self.assertFalse(unknown.is_valid)
        self.assertTrue(any("bad" in error for error in unknown.errors))

    async def test_run_query_attempts_returns_iteration_log(self) -> None:
        result = await run_query_attempts(
            question="Which places exist?",
            generation_prompt="prompt",
            ontology_context={
                "object_properties": [{"uri": "http://example.com/worksAt"}],
                "datatype_properties": [],
                "classes": [],
                "prefixes": [],
            },
            endpoint_url="http://example.test/dataset/query",
            model="stub-model",
            llm_api_url="http://example.test/llm",
            k_max=3,
        )

        self.assertEqual(result.status, "completed")
        self.assertIn("SELECT", result.final_query)
        self.assertGreaterEqual(len(result.iterations), 1)
        self.assertEqual(result.iterations[0]["execution"]["code"], "EXECUTION_OK")

    async def test_active_package_pointer_can_drive_runtime_pipeline(self) -> None:
        await self._prepare_file_package()
        build_index(self.package_dir, chunking="class_based")

        packages_root = self.root / "ontology_packages-root"
        package_module.set_active_package(packages_root, self.package_dir)
        result = await run_query_pipeline(
            "Which places exist?",
            get_active_package(packages_root),
            model="stub-model",
        )
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.dataset_endpoint, "http://example.test/dataset/query")
        trace_payload = json.loads(Path(result.trace_path).read_text(encoding="utf-8"))
        latest_trace = trace_payload[-1]
        self.assertIn("correction_iterations", latest_trace)
        self.assertIn("run_id", latest_trace)
        self.assertIn("readable_trace_path", result.to_dict())
        self.assertEqual(latest_trace["correction_iterations"][0]["validation"]["is_valid"], True)
        self.assertEqual(latest_trace["correction_iterations"][0]["status"], "completed")
        self.assertEqual(latest_trace["correction_iterations"][0]["validation_summary"], "VALIDATION_OK")
        self.assertEqual(latest_trace["correction_iterations"][0]["errors"], [])
        self.assertEqual(latest_trace["correction_iterations"][0]["execution"]["code"], "EXECUTION_OK")

if __name__ == "__main__":
    unittest.main()
