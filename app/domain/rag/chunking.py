"""Chunk builders for ontology index construction."""

from __future__ import annotations


SUPPORTED_CHUNKING_STRATEGIES = {"class_based", "property_based", "composite"}
SUPPORTED_CHUNKING_ORDER = ("class_based", "property_based", "composite")


def build_chunks(ontology_context: dict[str, object], chunking: str) -> list[dict[str, object]]:
    """Build retrieval chunks for the selected strategy."""
    if chunking == "class_based":
        return _build_class_based_chunks(ontology_context)
    if chunking == "property_based":
        return _build_property_based_chunks(ontology_context)
    if chunking == "composite":
        return _build_composite_chunks(ontology_context)
    raise ValueError(f"Unsupported chunking strategy: {chunking}")


def _build_class_based_chunks(ontology_context: dict[str, object]) -> list[dict[str, object]]:
    class_chunks: list[dict[str, object]] = []

    classes = ontology_context.get("classes", [])
    object_properties = ontology_context.get("object_properties", [])
    datatype_properties = ontology_context.get("datatype_properties", [])

    if not isinstance(classes, list):
        return class_chunks

    for class_data in classes:
        if not isinstance(class_data, dict):
            continue
        class_name = class_data.get("name") or class_data.get("label") or class_data.get("uri")
        if not class_name:
            continue

        class_name = _short_name(str(class_name))
        class_label = class_data.get("label")
        class_label = class_label.strip() if isinstance(class_label, str) else None
        description = _description_for(class_data, class_name)
        parent_classes = [
            _short_name(parent_class)
            for parent_class in class_data.get("parent_classes", [])
            if isinstance(parent_class, str) and parent_class
        ]
        class_uri = class_data.get("uri")

        class_object_properties = _properties_for_class(
            properties=object_properties if isinstance(object_properties, list) else [],
            class_uri=class_uri if isinstance(class_uri, str) else None,
        )
        class_datatype_properties = _properties_for_class(
            properties=datatype_properties if isinstance(datatype_properties, list) else [],
            class_uri=class_uri if isinstance(class_uri, str) else None,
        )

        text = _build_class_chunk_text(
            class_name=class_name,
            class_label=class_label,
            description=description,
            object_properties=class_object_properties,
            datatype_properties=class_datatype_properties,
        )

        class_chunks.append(
            {
                "chunk_type": "class",
                "class_name": class_name,
                "class_label": class_label,
                "class_uri": class_uri,
                "text": text,
                "metadata": {
                    "label": class_label,
                    "description": description,
                    "object_properties": class_object_properties,
                    "datatype_properties": class_datatype_properties,
                },
            }
        )

    return class_chunks


def _build_property_based_chunks(ontology_context: dict[str, object]) -> list[dict[str, object]]:
    property_chunks: list[dict[str, object]] = []
    classes = ontology_context.get("classes", [])
    class_names_by_uri = _class_names_by_uri(classes if isinstance(classes, list) else [])
    properties = _all_properties(ontology_context)

    for property_data in properties:
        property_uri = property_data.get("uri")
        property_name = property_data.get("name") or property_data.get("label") or property_uri
        if not property_name:
            continue

        property_name = _short_name(str(property_name))
        property_label = _clean_text(property_data.get("label"))
        description = _description_for(property_data, property_name)
        property_type = _clean_text(property_data.get("property_type")) or "property"
        domains = _named_uri_values(property_data.get("domain"), class_names_by_uri)
        ranges = _named_uri_values(property_data.get("range"), class_names_by_uri)

        text = _build_property_chunk_text(
            property_name=property_name,
            property_label=property_label,
            description=description,
            property_type=property_type,
            domains=domains,
            ranges=ranges,
        )

        property_chunks.append(
            {
                "chunk_type": "property",
                "property_name": property_name,
                "property_label": property_label,
                "property_uri": property_uri,
                "text": text,
                "metadata": {
                    "label": property_label,
                    "description": description,
                    "property_type": property_type,
                    "domain": domains,
                    "range": ranges,
                },
            }
        )

    return property_chunks


def _build_composite_chunks(ontology_context: dict[str, object]) -> list[dict[str, object]]:
    composite_chunks: list[dict[str, object]] = []
    classes = ontology_context.get("classes", [])
    object_properties = ontology_context.get("object_properties", [])
    datatype_properties = ontology_context.get("datatype_properties", [])

    if not isinstance(classes, list):
        return composite_chunks

    children_by_uri = _child_classes_by_uri(classes)
    class_names_by_uri = _class_names_by_uri(classes)

    for class_data in classes:
        if not isinstance(class_data, dict):
            continue
        class_uri = class_data.get("uri")
        class_name = class_data.get("name") or class_data.get("label") or class_uri
        if not class_name:
            continue

        class_uri_text = class_uri if isinstance(class_uri, str) else None
        class_name = _short_name(str(class_name))
        class_label = _clean_text(class_data.get("label"))
        description = _description_for(class_data, class_name)
        parent_classes = _named_uri_values(class_data.get("parent_classes"), class_names_by_uri)
        child_classes = [
            class_names_by_uri.get(child_uri, _short_name(child_uri))
            for child_uri in children_by_uri.get(class_uri_text or "", [])
        ]
        class_object_properties = _properties_for_class(
            properties=object_properties if isinstance(object_properties, list) else [],
            class_uri=class_uri_text,
        )
        class_datatype_properties = _properties_for_class(
            properties=datatype_properties if isinstance(datatype_properties, list) else [],
            class_uri=class_uri_text,
        )
        incoming_object_properties = _incoming_properties_for_class(
            properties=object_properties if isinstance(object_properties, list) else [],
            class_uri=class_uri_text,
            class_names_by_uri=class_names_by_uri,
        )

        text = _build_composite_chunk_text(
            class_name=class_name,
            class_label=class_label,
            description=description,
            parent_classes=parent_classes,
            child_classes=child_classes,
            object_properties=class_object_properties,
            datatype_properties=class_datatype_properties,
            incoming_object_properties=incoming_object_properties,
        )

        composite_chunks.append(
            {
                "chunk_type": "composite",
                "class_name": class_name,
                "class_label": class_label,
                "class_uri": class_uri,
                "text": text,
                "metadata": {
                    "label": class_label,
                    "description": description,
                    "parent_classes": parent_classes,
                    "child_classes": child_classes,
                    "object_properties": class_object_properties,
                    "datatype_properties": class_datatype_properties,
                    "incoming_object_properties": incoming_object_properties,
                },
            }
        )

    return composite_chunks


def _all_properties(ontology_context: dict[str, object]) -> list[dict[str, object]]:
    properties: list[dict[str, object]] = []
    for key in ("object_properties", "datatype_properties"):
        value = ontology_context.get(key, [])
        if not isinstance(value, list):
            continue
        properties.extend(item for item in value if isinstance(item, dict))
    return properties


def _class_names_by_uri(classes: list[dict[str, object]]) -> dict[str, str]:
    names: dict[str, str] = {}
    for class_data in classes:
        class_uri = class_data.get("uri")
        class_name = class_data.get("name") or class_data.get("label") or class_uri
        if isinstance(class_uri, str) and class_uri and class_name:
            names[class_uri] = _short_name(str(class_name))
    return names


def _child_classes_by_uri(classes: list[dict[str, object]]) -> dict[str, list[str]]:
    child_map: dict[str, list[str]] = {}
    for class_data in classes:
        child_uri = class_data.get("uri")
        if not isinstance(child_uri, str) or not child_uri:
            continue
        for parent_uri in class_data.get("parent_classes", []):
            if isinstance(parent_uri, str) and parent_uri:
                child_map.setdefault(parent_uri, []).append(child_uri)
    return {parent_uri: sorted(child_uris) for parent_uri, child_uris in child_map.items()}


def _properties_for_class(properties: list[dict[str, object]], class_uri: str | None) -> list[str]:
    if not class_uri:
        return []

    related = []
    for property_data in properties:
        domains = property_data.get("domain", [])
        if not isinstance(domains, list) or class_uri not in domains:
            continue
        property_name = property_data.get("name") or property_data.get("label") or property_data.get("uri")
        if not property_name:
            continue
        ranges = property_data.get("range", [])
        range_name = "Unknown"
        if isinstance(ranges, list) and ranges:
            first_range = ranges[0]
            if first_range:
                range_name = _short_name(str(first_range))
        related.append(f"{_short_name(str(property_name))} -> {range_name}")
    return sorted(set(related))


def _incoming_properties_for_class(
    properties: list[dict[str, object]],
    class_uri: str | None,
    class_names_by_uri: dict[str, str],
) -> list[str]:
    """Find properties where this class appears as the range (i.e., is the target).

    Returns lines like: "providesTrainingCourse (from TrainingCentre)"
    """
    if not class_uri:
        return []

    incoming = []
    for property_data in properties:
        ranges = property_data.get("range", [])
        if not isinstance(ranges, list) or class_uri not in ranges:
            continue
        property_name = property_data.get("name") or property_data.get("label") or property_data.get("uri")
        if not property_name:
            continue
        domains = property_data.get("domain", [])
        source_names = []
        if isinstance(domains, list):
            for domain_uri in domains:
                if isinstance(domain_uri, str):
                    source_names.append(class_names_by_uri.get(domain_uri, _short_name(domain_uri)))
        source_text = ", ".join(source_names) if source_names else "unknown source"
        incoming.append(f"{_short_name(str(property_name))} (from {source_text})")
    return sorted(set(incoming))


def _build_class_chunk_text(
    *,
    class_name: str,
    class_label: str | None,
    description: str,
    object_properties: list[str],
    datatype_properties: list[str],
) -> str:
    object_property_text = _bullet_list(object_properties)
    datatype_property_text = _bullet_list(datatype_properties)
    description_text = description or "No description available."
    label_text = class_label or "No label available."

    return (
        f"Class: {class_name}\n\n"
        f"Label: {label_text}\n\n"
        f"Description: {description_text}\n\n"
        f"Object Properties:\n{object_property_text}\n\n"
        f"Datatype Properties:\n{datatype_property_text}"
    )


def _build_property_chunk_text(
    *,
    property_name: str,
    property_label: str | None,
    description: str,
    property_type: str,
    domains: list[str],
    ranges: list[str],
) -> str:
    label_text = property_label or "No label available."
    description_text = description or "No description available."
    return (
        f"Property: {property_name}\n\n"
        f"Label: {label_text}\n\n"
        f"Description: {description_text}\n\n"
        f"Type: {property_type}\n\n"
        f"Domain Classes:\n{_bullet_list(domains)}\n\n"
        f"Range Classes or Datatypes:\n{_bullet_list(ranges)}"
    )


def _build_composite_chunk_text(
    *,
    class_name: str,
    class_label: str | None,
    description: str,
    parent_classes: list[str],
    child_classes: list[str],
    object_properties: list[str],
    datatype_properties: list[str],
    incoming_object_properties: list[str] | None = None,
) -> str:
    label_text = class_label or "No label available."
    description_text = description or "No description available."
    incoming_text = _bullet_list(incoming_object_properties or [])
    return (
        f"Class Neighbourhood: {class_name}\n\n"
        f"Label: {label_text}\n\n"
        f"Description: {description_text}\n\n"
        f"Parent Classes:\n{_bullet_list(parent_classes, empty_label='None (top-level class)')}\n\n"
        f"Child Classes:\n{_bullet_list(child_classes)}\n\n"
        f"Direct Object Properties:\n{_bullet_list(object_properties)}\n\n"
        f"Direct Datatype Properties:\n{_bullet_list(datatype_properties)}\n\n"
        f"Incoming Object Properties (this class is the target):\n{incoming_text}"
    )


def _description_for(class_data: dict[str, object], class_name: str) -> str:
    comment = class_data.get("comment")
    description = comment.strip() if isinstance(comment, str) else ""
    if description:
        return description

    label = class_data.get("label")
    if isinstance(label, str):
        normalized = label.strip()
        if normalized and normalized != class_name:
            return normalized

    return "No description available"


def _named_uri_values(value: object, class_names_by_uri: dict[str, str]) -> list[str]:
    if not isinstance(value, list):
        return []
    return sorted(
        {
            class_names_by_uri.get(item, _short_name(item))
            for item in value
            if isinstance(item, str) and item
        }
    )


def _clean_text(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _short_name(value: str) -> str:
    if "#" in value:
        return value.rsplit("#", 1)[-1]
    if "/" in value:
        return value.rstrip("/").rsplit("/", 1)[-1]
    if ":" in value:
        return value.rsplit(":", 1)[-1]
    return value


def _bullet_list(values: list[str], empty_label: str = "None") -> str:
    if not values:
        return f"- {empty_label}"
    return "\n".join(f"- {value}" for value in values)
