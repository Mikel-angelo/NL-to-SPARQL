"""
Responsible for the early ontology interpretation stage before full context extraction.

Functions:
    • parse uploaded ontology content into an initial RDF graph
    • run fast detection counts for classes, properties, and instances
    • classify the ontology as schema-only, mixed, or instances-only
    • resolve external schemas heuristically for instances-only inputs
    • build the final graph from the original ontology plus resolved schemas

Outputs:
    • initial RDF graph
    • detection counts
    • ontology mode
    • resolved schema files
    • final RDF graph
"""

from dataclasses import dataclass
from io import BytesIO

import httpx
from rdflib import Graph, RDF, RDFS, URIRef
from rdflib.namespace import OWL


RDFLIB_FORMATS = {
    ".ttl": "turtle",
    ".owl": "xml",
    ".rdf": "xml",
}

SCHEMA_TYPE_URIS = {
    str(OWL.Class),
    str(RDFS.Class),
    str(OWL.ObjectProperty),
    str(OWL.DatatypeProperty),
    str(OWL.AnnotationProperty),
    str(OWL.Ontology),
    str(RDF.Property),
}

@dataclass(frozen=True)
class DetectionResult:
    """Lightweight graph statistics used for mode classification."""

    classes_count: int
    properties_count: int
    instances_count: int


@dataclass(frozen=True)
class ResolvedSchemaFile:
    """One externally resolved schema file stored locally for the current run."""

    source_namespace: str
    url: str
    filename: str
    content: bytes
    suffix: str


@dataclass(frozen=True)
class SchemaResolutionResult:
    """Outcome of the optional schema-resolution step."""

    resolved_files: list[ResolvedSchemaFile]
    attempted_urls: list[str]
    failed_urls: list[str]


class OntologySchemaResolutionService:
    """Parse the uploaded ontology, detect its mode, and optionally resolve schemas."""

    async def parse_uploaded_content(self, content: bytes, suffix: str) -> Graph:
        """Parse the uploaded ontology file into the initial RDF graph."""
        graph = Graph()
        graph.parse(source=BytesIO(content), format=RDFLIB_FORMATS[suffix])
        return graph

    def detect(self, graph: Graph) -> DetectionResult:
        """Run a fast graph scan used to classify the ontology file."""
        class_uris = self._subjects_for_types(graph, {OWL.Class})
        object_property_uris = self._subjects_for_types(graph, {OWL.ObjectProperty})
        datatype_property_uris = self._subjects_for_types(graph, {OWL.DatatypeProperty})

        instances: set[str] = set()
        for subject, rdf_type in graph.subject_objects(RDF.type):
            if not isinstance(subject, URIRef) or not isinstance(rdf_type, URIRef):
                continue
            if str(rdf_type) in SCHEMA_TYPE_URIS:
                continue
            instances.add(str(subject))

        return DetectionResult(
            classes_count=len(class_uris),
            properties_count=len(object_property_uris) + len(datatype_property_uris),
            instances_count=len(instances),
        )

    def classify_mode(self, detection: DetectionResult) -> str:
        """Map fast graph counts to one of the supported ontology modes."""
        if detection.classes_count > 0 and detection.instances_count == 0:
            return "schema-only"
        if detection.classes_count > 0 and detection.instances_count > 0:
            return "mixed"
        if detection.classes_count == 0 and detection.instances_count > 0:
            return "instances-only"
        return "schema-only"

    async def resolve_schemas(self, graph: Graph) -> SchemaResolutionResult:
        """Try to download external schemas referenced by rdf:type in instances-only files."""
        namespaces = self._external_type_namespaces(graph)
        attempted_urls: list[str] = []
        failed_urls: list[str] = []
        resolved_files: list[ResolvedSchemaFile] = []
        seen_urls: set[str] = set()

        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            for namespace in namespaces:
                for url in self._candidate_schema_urls(namespace):
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    attempted_urls.append(url)
                    try:
                        response = await client.get(url)
                    except httpx.HTTPError:
                        continue
                    if response.status_code >= 400:
                        continue

                    suffix = self._suffix_from_url_or_type(url, response.headers.get("content-type", ""))
                    if suffix not in RDFLIB_FORMATS:
                        continue

                    try:
                        schema_graph = Graph()
                        schema_graph.parse(source=BytesIO(response.content), format=RDFLIB_FORMATS[suffix])
                    except Exception:
                        failed_urls.append(url)
                        continue

                    if len(schema_graph) == 0:
                        failed_urls.append(url)
                        continue

                    filename = f"schema-{len(resolved_files) + 1}{suffix}"
                    resolved_files.append(
                        ResolvedSchemaFile(
                            source_namespace=namespace,
                            url=url,
                            filename=filename,
                            content=response.content,
                            suffix=suffix,
                        )
                    )
                    break

        return SchemaResolutionResult(
            resolved_files=resolved_files,
            attempted_urls=attempted_urls,
            failed_urls=failed_urls,
        )

    def build_final_graph(self, original_graph: Graph, schema_files: list[ResolvedSchemaFile]) -> Graph:
        """Combine the uploaded ontology graph with any resolved schemas."""
        final_graph = Graph()
        for triple in original_graph:
            final_graph.add(triple)

        for schema_file in schema_files:
            schema_graph = Graph()
            schema_graph.parse(source=BytesIO(schema_file.content), format=RDFLIB_FORMATS[schema_file.suffix])
            for triple in schema_graph:
                final_graph.add(triple)

        return final_graph

    def _external_type_namespaces(self, graph: Graph) -> list[str]:
        namespaces: set[str] = set()
        for _, rdf_type in graph.subject_objects(RDF.type):
            if not isinstance(rdf_type, URIRef):
                continue
            if str(rdf_type) in SCHEMA_TYPE_URIS:
                continue
            namespace = self._namespace_of(rdf_type)
            if namespace:
                namespaces.add(namespace)
        return sorted(namespaces)

    @staticmethod
    def _candidate_schema_urls(namespace: str) -> list[str]:
        base = namespace.rstrip("#/")
        return [
            namespace,
            base,
            f"{base}.owl",
            f"{base}.rdf",
            f"{base}.ttl",
        ]

    @staticmethod
    def _suffix_from_url_or_type(url: str, content_type: str) -> str | None:
        lowered_type = content_type.lower()
        lowered_url = url.lower()
        if "text/turtle" in lowered_type or lowered_url.endswith(".ttl"):
            return ".ttl"
        if "rdf+xml" in lowered_type or lowered_url.endswith(".owl") or lowered_url.endswith(".rdf"):
            return ".owl" if lowered_url.endswith(".owl") else ".rdf"
        return None

    @staticmethod
    def _namespace_of(subject: URIRef) -> str | None:
        text = str(subject)
        if "#" in text:
            head, _, _ = text.rpartition("#")
            return f"{head}#"
        if "/" in text:
            head, _, _ = text.rpartition("/")
            return f"{head}/"
        return None

    @staticmethod
    def _subjects_for_types(graph: Graph, rdf_types: set[URIRef]) -> list[URIRef]:
        subjects: set[URIRef] = set()
        for rdf_type in rdf_types:
            for subject in graph.subjects(RDF.type, rdf_type):
                if isinstance(subject, URIRef):
                    subjects.add(subject)
        return sorted(subjects, key=str)
