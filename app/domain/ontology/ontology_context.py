"""Build the `ontology_context.json` payload from an RDF graph.

This module converts a prepared RDFLib graph into the runtime ontology context:
prefixes, classes, object/datatype properties, class hierarchy, and instance
statistics. It does not write the JSON file itself.
"""

from __future__ import annotations

from rdflib import Graph, Literal, RDF, RDFS, URIRef
from rdflib.namespace import OWL


def build_ontology_context(
    graph: Graph,
    *,
    ontology_name: str,
    source_filename: str,
) -> dict[str, object]:
    """Return the ontology context stored in `ontology_context.json`."""
    class_uris = _subjects_for_types(graph, {OWL.Class, RDFS.Class})
    object_property_uris = _subjects_for_types(graph, {OWL.ObjectProperty})
    datatype_property_uris = _subjects_for_types(graph, {OWL.DatatypeProperty})

    return {
        "ontology_name": ontology_name,
        "source_filename": source_filename,
        "triple_count": len(graph),
        "prefixes": _prefixes(graph),
        "classes": [_class_entry(graph, subject) for subject in class_uris],
        "object_properties": [
            _property_entry(graph, subject, "object_property") for subject in object_property_uris
        ],
        "datatype_properties": [
            _property_entry(graph, subject, "datatype_property") for subject in datatype_property_uris
        ],
        "class_hierarchy": _class_hierarchy(graph, class_uris),
        "instance_statistics": _instance_statistics(graph, class_uris),
    }


def _class_entry(graph: Graph, subject: URIRef) -> dict[str, object]:
    return {
        "uri": str(subject),
        "name": _local_name(subject),
        "label": _label_for(graph, subject),
        "comment": _comment_for(graph, subject),
        "parent_classes": sorted(
            str(parent)
            for parent in graph.objects(subject, RDFS.subClassOf)
            if isinstance(parent, URIRef)
        ),
    }


def _property_entry(graph: Graph, subject: URIRef, property_type: str) -> dict[str, object]:
    return {
        "uri": str(subject),
        "name": _local_name(subject),
        "label": _label_for(graph, subject),
        "comment": _comment_for(graph, subject),
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


def _class_hierarchy(graph: Graph, class_uris: list[URIRef]) -> list[dict[str, str]]:
    hierarchy = []
    for child in class_uris:
        for parent in graph.objects(child, RDFS.subClassOf):
            if isinstance(parent, URIRef):
                hierarchy.append({"parent": str(parent), "child": str(child)})
    return sorted(hierarchy, key=lambda item: (item["parent"], item["child"]))


def _instance_statistics(graph: Graph, class_uris: list[URIRef]) -> dict[str, object]:
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


def _subjects_for_types(graph: Graph, rdf_types: set[URIRef]) -> list[URIRef]:
    subjects: set[URIRef] = set()
    for rdf_type in rdf_types:
        for subject in graph.subjects(RDF.type, rdf_type):
            if isinstance(subject, URIRef):
                subjects.add(subject)
    return sorted(subjects, key=str)


def _prefixes(graph: Graph) -> list[dict[str, str]]:
    used_namespaces = _used_namespaces(graph)
    prefixes: list[dict[str, str]] = []

    for prefix, namespace in graph.namespaces():
        namespace_text = str(namespace)
        if namespace_text not in used_namespaces:
            continue
        prefixes.append(
            {
                "prefix": ":" if prefix == "" else prefix,
                "namespace": namespace_text,
            }
        )

    return sorted(prefixes, key=lambda item: item["prefix"])


def _used_namespaces(graph: Graph) -> set[str]:
    namespaces: set[str] = set()
    for subject, predicate, object_value in graph:
        for value in (subject, predicate, object_value):
            if isinstance(value, URIRef):
                namespace = _namespace_for(value)
                if namespace:
                    namespaces.add(namespace)
    return namespaces


def _namespace_for(uri: URIRef) -> str:
    text = str(uri)
    if "#" in text:
        return text.rsplit("#", 1)[0] + "#"
    if "/" in text:
        return text.rsplit("/", 1)[0] + "/"
    return ""


def _label_for(graph: Graph, subject: URIRef) -> str:
    label = graph.value(subject, RDFS.label)
    if isinstance(label, Literal):
        return str(label)
    return _local_name(subject)


def _comment_for(graph: Graph, subject: URIRef) -> str | None:
    comment = graph.value(subject, RDFS.comment)
    if isinstance(comment, Literal):
        return str(comment)
    return None


def _local_name(subject: URIRef) -> str:
    text = str(subject)
    if "#" in text:
        return text.rsplit("#", 1)[-1]
    return text.rstrip("/").rsplit("/", 1)[-1]
