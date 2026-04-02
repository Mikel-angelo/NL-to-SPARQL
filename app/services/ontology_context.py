import json
from io import BytesIO
from pathlib import Path

from rdflib import Graph, Literal, RDF, RDFS, URIRef
from rdflib.namespace import OWL


RDFLIB_FORMATS = {
    ".ttl": "turtle",
    ".owl": "xml",
    ".rdf": "xml",
}


class OntologyContextService:
    """Builds the normalized semantic representation of an ontology."""

    def extract_from_content(
        self,
        content: bytes,
        suffix: str,
        ontology_name: str,
        source_filename: str,
    ) -> dict[str, object]:
        """Parses ontology content and returns the normalized ontology context."""
        graph = Graph()
        graph.parse(source=BytesIO(content), format=RDFLIB_FORMATS[suffix])
        return self._build_ontology_context(graph, ontology_name, source_filename)

    def _build_ontology_context(
        self,
        graph: Graph,
        ontology_name: str,
        source_filename: str,
    ) -> dict[str, object]:
        class_uris = self._subjects_for_types(graph, {OWL.Class, RDFS.Class})
        object_property_uris = self._subjects_for_types(graph, {OWL.ObjectProperty})
        datatype_property_uris = self._subjects_for_types(graph, {OWL.DatatypeProperty})

        return {
            "ontology_name": ontology_name,
            "source_filename": source_filename,
            "triple_count": len(graph),
            "prefixes": self._prefixes(graph),
            "classes": [self._class_entry(graph, subject) for subject in class_uris],
            "object_properties": [self._property_entry(graph, subject, "object_property") for subject in object_property_uris],
            "datatype_properties": [self._property_entry(graph, subject, "datatype_property") for subject in datatype_property_uris],
            "class_hierarchy": self._class_hierarchy(graph, class_uris),
            "instance_statistics": self._instance_statistics(graph, class_uris),
        }

    def _class_entry(self, graph: Graph, subject: URIRef) -> dict[str, object]:
        return {
            "uri": str(subject),
            "name": self._local_name(subject),
            "label": self._label_for(graph, subject),
            "comment": self._comment_for(graph, subject),
            "parent_classes": sorted(
                str(parent)
                for parent in graph.objects(subject, RDFS.subClassOf)
                if isinstance(parent, URIRef)
            ),
        }

    def _property_entry(self, graph: Graph, subject: URIRef, property_type: str) -> dict[str, object]:
        return {
            "uri": str(subject),
            "name": self._local_name(subject),
            "label": self._label_for(graph, subject),
            "comment": self._comment_for(graph, subject),
            "domain": sorted(
                str(domain)
                for domain in graph.objects(subject, RDFS.domain)
                if isinstance(domain, URIRef)
            ),
            "range": sorted(
                str(range_value)
                for range_value in graph.objects(subject, RDFS.range)
                if isinstance(range_value, URIRef)
            ),
            "property_type": property_type,
        }

    def _class_hierarchy(self, graph: Graph, class_uris: list[URIRef]) -> list[dict[str, str]]:
        hierarchy = []
        for child in class_uris:
            for parent in graph.objects(child, RDFS.subClassOf):
                if isinstance(parent, URIRef):
                    hierarchy.append({"parent": str(parent), "child": str(child)})
        return sorted(hierarchy, key=lambda item: (item["parent"], item["child"]))

    def _instance_statistics(self, graph: Graph, class_uris: list[URIRef]) -> dict[str, object]:
        class_instances = []
        total_instances = 0
        for class_uri in class_uris:
            count = sum(1 for _ in graph.subjects(RDF.type, class_uri))
            if count:
                class_instances.append({"class_uri": str(class_uri), "count": count})
                total_instances += count
        return {
            "total_instances": total_instances,
            "class_instances": sorted(class_instances, key=lambda item: item["class_uri"]),
        }

    @staticmethod
    def _subjects_for_types(graph: Graph, rdf_types: set[URIRef]) -> list[URIRef]:
        subjects: set[URIRef] = set()
        for rdf_type in rdf_types:
            for subject in graph.subjects(RDF.type, rdf_type):
                if isinstance(subject, URIRef):
                    subjects.add(subject)
        return sorted(subjects, key=str)

    @staticmethod
    def _prefixes(graph: Graph) -> list[dict[str, str]]:
        return sorted(
            [{"prefix": prefix, "namespace": str(namespace)} for prefix, namespace in graph.namespaces()],
            key=lambda item: item["prefix"],
        )

    @staticmethod
    def _label_for(graph: Graph, subject: URIRef) -> str:
        label = graph.value(subject, RDFS.label)
        if isinstance(label, Literal):
            return str(label)
        return OntologyContextService._local_name(subject)

    @staticmethod
    def _comment_for(graph: Graph, subject: URIRef) -> str | None:
        comment = graph.value(subject, RDFS.comment)
        if isinstance(comment, Literal):
            return str(comment)
        return None

    @staticmethod
    def _local_name(subject: URIRef) -> str:
        text = str(subject)
        if "#" in text:
            return text.rsplit("#", 1)[-1]
        return text.rstrip("/").rsplit("/", 1)[-1]
