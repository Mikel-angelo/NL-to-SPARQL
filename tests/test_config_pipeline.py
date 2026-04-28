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
from app.domain.ontology import extract_metadata
from app.domain.package import get_active_package
from app.domain.rag import build_index, retrieve_text_chunks
from app.domain.rag.chunking import build_chunks
from app.domain.rag.retrieve_context import RetrievedChunk
from app.domain.runtime import generate_with_correction, run_query_pipeline
from app.domain.runtime import correction_loop as correction_loop_module
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
        self._patch(correction_loop_module, "generate_text", self._fake_generate_text)
        self._patch(correction_loop_module, "execute_sparql_query", self._fake_execute_query)

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

    async def test_file_extraction_and_runtime_artifacts_create_expected_layout(self) -> None:
        extraction = await extract_metadata(
            str(self.ontology_path),
            self.package_dir,
            source_mode="file",
            dataset_name="example-dataset",
            query_endpoint="http://example.test/dataset/query",
        )
        artifact_result = build_index(self.package_dir, chunking="class_based")

        self.assertEqual(extraction.metadata["source_mode"], "file")
        self.assertTrue((self.package_dir / "ontology").exists())
        self.assertTrue((self.package_dir / "metadata.json").exists())
        self.assertTrue((self.package_dir / "ontology_context.json").exists())
        self.assertTrue((self.package_dir / "settings.json").exists())
        self.assertTrue(artifact_result.chunks_path.exists())
        self.assertTrue(artifact_result.index_path.exists())
        self.assertEqual(artifact_result.chunks_path.parent.name, "chunks")
        self.assertEqual(artifact_result.index_path.parent.name, "chunks")

    async def test_chunking_strategies_build_class_property_and_composite_chunks(self) -> None:
        extraction = await extract_metadata(
            str(self.ontology_path),
            self.package_dir,
            source_mode="file",
            dataset_name="example-dataset",
            query_endpoint="http://example.test/dataset/query",
        )

        class_chunks = build_chunks(extraction.ontology_context, "class_based")
        property_chunks = build_chunks(extraction.ontology_context, "property_based")
        composite_chunks = build_chunks(extraction.ontology_context, "composite")

        self.assertTrue(any(chunk["chunk_type"] == "class" for chunk in class_chunks))
        self.assertTrue(any(chunk.get("property_name") == "worksAt" for chunk in property_chunks))
        self.assertTrue(any("Domain Classes:" in str(chunk.get("text")) for chunk in property_chunks))
        self.assertTrue(any("Place" in str(chunk.get("text")) for chunk in property_chunks))
        self.assertTrue(any(chunk.get("class_name") == "Person" for chunk in composite_chunks))
        self.assertTrue(any("Child Classes:" in str(chunk.get("text")) for chunk in composite_chunks))
        self.assertTrue(any("Employee" in str(chunk.get("text")) for chunk in composite_chunks))

    async def test_endpoint_extraction_creates_config_without_source_file(self) -> None:
        graph = Graph()
        graph.parse(data=ONTOLOGY_TTL, format="turtle")
        self._patch(
            __import__("app.domain.ontology.onboarding_extraction", fromlist=["_graph_from_sparql_endpoint"]),
            "_graph_from_sparql_endpoint",
            self._fake_graph_from_endpoint(graph),
        )

        extraction = await extract_metadata(
            "http://example.test/sparql",
            self.package_dir,
            source_mode="sparql_endpoint",
            query_endpoint="http://example.test/sparql",
        )
        build_index(self.package_dir, chunking="class_based")

        self.assertEqual(extraction.metadata["source_mode"], "sparql_endpoint")
        self.assertFalse(any(path.name.startswith("source.") for path in (self.package_dir / "ontology").glob("*")))
        self.assertEqual(extraction.settings["query_endpoint"], "http://example.test/sparql")

    async def test_run_query_pipeline_uses_config_artifacts(self) -> None:
        await extract_metadata(
            str(self.ontology_path),
            self.package_dir,
            source_mode="file",
            dataset_name="example-dataset",
            query_endpoint="http://example.test/dataset/query",
        )
        build_index(self.package_dir, chunking="class_based")

        result = await run_query_pipeline(
            "Which places exist?",
            self.package_dir,
            model="stub-model",
        )

        self.assertEqual(result.status, "completed")
        self.assertIn("SELECT", result.generated_sparql or "")
        self.assertTrue(Path(result.trace_path).exists())
        self.assertGreaterEqual(len(result.retrieved_context), 1)

    async def test_rag_retrieve_text_chunks_returns_plain_text(self) -> None:
        await extract_metadata(
            str(self.ontology_path),
            self.package_dir,
            source_mode="file",
            dataset_name="example-dataset",
            query_endpoint="http://example.test/dataset/query",
        )
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
        self.assertIn("Which places exist?", prompt)
        self.assertNotIn(":ActorType", prompt)

    def test_correction_prompt_renderer_uses_feedback_template(self) -> None:
        prompt = render_correction_prompt(
            original_question="Which places exist?",
            failed_query="SELECT ?x WHERE { ?s ?p ?o }",
            validation_errors=["SELECT variables are not bound in WHERE: x"],
        )

        self.assertIn("Original question:", prompt)
        self.assertIn("Failed query:", prompt)
        self.assertIn("SELECT variables are not bound in WHERE: x", prompt)

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

    async def test_generate_with_correction_returns_iteration_log(self) -> None:
        result = await generate_with_correction(
            question="Which places exist?",
            initial_prompt="prompt",
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
        await extract_metadata(
            str(self.ontology_path),
            self.package_dir,
            source_mode="file",
            dataset_name="example-dataset",
            query_endpoint="http://example.test/dataset/query",
        )
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
        self.assertEqual(latest_trace["correction_iterations"][0]["validation"]["is_valid"], True)
        self.assertEqual(latest_trace["correction_iterations"][0]["execution"]["code"], "EXECUTION_OK")

    @staticmethod
    def _fake_graph_from_endpoint(graph: Graph):
        async def _inner(endpoint: str) -> Graph:
            del endpoint
            return graph

        return _inner


if __name__ == "__main__":
    unittest.main()
