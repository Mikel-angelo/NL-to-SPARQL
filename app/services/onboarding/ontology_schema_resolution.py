"""
Responsible for the early ontology interpretation stage before full context extraction.

Functions:
    • parse uploaded ontology content into an initial RDF graph
    • run fast detection counts for classes, properties, and instances
    • classify the ontology as schema-only, mixed, or instances-only
    • analyze whether instance rdf:type values are covered by local class declarations
    • resolve external schemas heuristically for missing instance-type namespaces
    • build the final graph from the original ontology plus resolved schemas

Outputs:
    • initial RDF graph
    • detection counts
    • ontology mode
    • schema coverage result
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
class CoverageResult:
    """Describes whether the uploaded graph declares the class URIs used by its instances."""

    status: str
    instance_type_uris: list[str]
    declared_class_uris: list[str]
    missing_class_uris: list[str]
    missing_namespaces: list[str]


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
    """Parse the uploaded ontology, classify its structure, analyze coverage, and resolve schemas."""

    async def parse_uploaded_content(self, content: bytes, suffix: str) -> Graph:
        """Parse the uploaded ontology file into the initial RDF graph."""
        graph = Graph()
        graph.parse(source=BytesIO(content), format=RDFLIB_FORMATS[suffix])
        return graph

    def detect(self, graph: Graph) -> DetectionResult:
        """Run a fast graph scan used to classify the ontology file."""
        class_uris = self._subjects_for_types(graph, {OWL.Class, RDFS.Class})
        object_property_uris = self._subjects_for_types(graph, {OWL.ObjectProperty})
        datatype_property_uris = self._subjects_for_types(graph, {OWL.DatatypeProperty})

        instances = {
            str(subject)
            for subject in self._instance_subjects(graph)
        }

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

    def analyze_schema_coverage(self, graph: Graph) -> CoverageResult:
        """Compare instance rdf:type class URIs with locally declared classes."""
        instance_type_uris = self._instance_type_uris(graph)
        declared_class_uris = self._subjects_for_types(graph, {OWL.Class, RDFS.Class})
        declared_class_uri_strings = {str(class_uri) for class_uri in declared_class_uris}

        missing_class_uris = sorted(
            str(class_uri)
            for class_uri in instance_type_uris
            if str(class_uri) not in declared_class_uri_strings
        )
        missing_namespaces = sorted(
            namespace
            for namespace in {
                self._namespace_of(URIRef(class_uri))
                for class_uri in missing_class_uris
            }
            if namespace
        )

        return CoverageResult(
            status="complete" if not missing_class_uris else "incomplete",
            instance_type_uris=sorted(str(class_uri) for class_uri in instance_type_uris),
            declared_class_uris=sorted(str(class_uri) for class_uri in declared_class_uris),
            missing_class_uris=missing_class_uris,
            missing_namespaces=missing_namespaces,
        )

    async def resolve_schemas(self, graph: Graph) -> SchemaResolutionResult:
        """Resolve schemas for any instance-type namespaces missing from the local graph."""
        coverage = self.analyze_schema_coverage(graph)
        return await self.resolve_schemas_for_namespaces(coverage.missing_namespaces)

    async def resolve_schemas_for_namespaces(self, namespaces: list[str]) -> SchemaResolutionResult:
        """Try to download RDF schemas for the provided missing class namespaces."""
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

    @staticmethod
    def _instance_subjects(graph: Graph) -> set[URIRef]:
        """Return subjects that look like domain instances rather than schema declarations."""
        instances: set[URIRef] = set()
        for subject, rdf_type in graph.subject_objects(RDF.type):
            if not isinstance(subject, URIRef) or not isinstance(rdf_type, URIRef):
                continue
            if str(rdf_type) in SCHEMA_TYPE_URIS:
                continue
            instances.add(subject)
        return instances

    def _instance_type_uris(self, graph: Graph) -> list[URIRef]:
        """Return the distinct class URIs used in rdf:type assertions for domain instances."""
        instance_subjects = self._instance_subjects(graph)
        instance_type_uris: set[URIRef] = set()
        for subject in instance_subjects:
            for rdf_type in graph.objects(subject, RDF.type):
                if not isinstance(rdf_type, URIRef):
                    continue
                if str(rdf_type) in SCHEMA_TYPE_URIS:
                    continue
                instance_type_uris.add(rdf_type)
        return sorted(instance_type_uris, key=str)

    @staticmethod
    def _candidate_schema_urls(namespace: str) -> list[str]:
        """Build a small set of likely schema-document URLs for one namespace."""
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
        """Infer which RDF parser to use from the response content type or URL suffix."""
        lowered_type = content_type.lower()
        lowered_url = url.lower()
        if "text/turtle" in lowered_type or lowered_url.endswith(".ttl"):
            return ".ttl"
        if "rdf+xml" in lowered_type or lowered_url.endswith(".owl") or lowered_url.endswith(".rdf"):
            return ".owl" if lowered_url.endswith(".owl") else ".rdf"
        return None

    @staticmethod
    def _namespace_of(subject: URIRef) -> str | None:
        """Extract the namespace portion of a URIRef using '#' or the final '/'."""
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
        """Collect distinct URI subjects declared with any of the requested rdf:type values."""
        subjects: set[URIRef] = set()
        for rdf_type in rdf_types:
            for subject in graph.subjects(RDF.type, rdf_type):
                if isinstance(subject, URIRef):
                    subjects.add(subject)
        return sorted(subjects, key=str)
