"""
Build retrieval chunks from ontology context data.

The first responsibility of this service is to transform `ontology_context.json`
into chunkable text records for later indexing.
"""

from pathlib import Path
import json

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

from app.core.config import settings


class RAGIndexService:
    """Create retrieval chunks from ontology context data."""

    def __init__(self, storage_dir: Path | None = None) -> None:
        self._storage_dir = storage_dir or Path(settings.storage_path)
        self._current_dir = self._storage_dir / "current"
        self._class_chunks_path = self._current_dir / "class_chunks.json"
        self._vector_index_path = self._current_dir / "index.faiss"
        self._embedding_model: SentenceTransformer | None = None

    def create_class_chunks(self, ontology_context: dict) -> list[dict[str, object]]:
        """Create and persist one text chunk per class from list-based `ontology_context.json`."""
        class_chunks: list[dict[str, object]] = []

        classes = ontology_context.get("classes", [])
        object_properties = ontology_context.get("object_properties", [])
        datatype_properties = ontology_context.get("datatype_properties", [])
        parent_classes_by_uri = self._parent_classes_by_uri(classes)

        for class_data in classes:
            class_name = class_data.get("name") or class_data.get("label") or class_data.get("uri")
            if not class_name:
                continue

            class_name = self._short_name(class_name)
            class_label = class_data.get("label")
            if isinstance(class_label, str):
                class_label = class_label.strip()
            else:
                class_label = None
            description = self._description_for(class_data, class_name)
            parent_classes = [
                self._short_name(parent_class)
                for parent_class in class_data.get("parent_classes", [])
                if isinstance(parent_class, str) and parent_class
            ]
            class_uri = class_data.get("uri")

            class_object_properties = self._properties_for_class_and_parents(
                properties=object_properties,
                class_uri=class_uri,
                parent_classes_by_uri=parent_classes_by_uri,
            )
            class_datatype_properties = self._properties_for_class_and_parents(
                properties=datatype_properties,
                class_uri=class_uri,
                parent_classes_by_uri=parent_classes_by_uri,
            )

            text = self._build_chunk_text(
                class_name=class_name,
                class_label=class_label,
                description=description,
                parent_classes=parent_classes,
                object_properties=class_object_properties,
                datatype_properties=class_datatype_properties,
            )

            class_chunks.append(
                {
                    "class_name": class_name,
                    "class_label": class_label,
                    "class_uri": class_uri,
                    "text": text,
                    "metadata": {
                        "label": class_label,
                        "description": description,
                        "parent_classes": parent_classes,
                        "object_properties": class_object_properties,
                        "datatype_properties": class_datatype_properties,
                    },
                }
            )

        self._current_dir.mkdir(parents=True, exist_ok=True)
        self._class_chunks_path.write_text(json.dumps(class_chunks, indent=2), encoding="utf-8")
        return class_chunks

    def extract_texts(self, class_chunks: list[dict[str, object]]) -> list[str]:
        """Extract chunk texts in the same order as the class chunk list."""
        return [chunk["text"] for chunk in class_chunks]

    def create_vector_index(self, class_chunks: list[dict[str, object]]) -> faiss.Index:
        """Run the static embedding and FAISS indexing pipeline for class chunks."""
        texts = self.extract_texts(class_chunks)
        vectors = self.embed_chunks(texts)
        index = self.build_vector_index(vectors)
        self.save_vector_index(index)
        return index

    def embed_chunks(self, texts: list[str]) -> np.ndarray:
        """Generate one embedding vector per chunk text."""
        if not texts:
            return np.empty((0, 0), dtype="float32")

        embeddings = self._embedding_model_instance().encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        return np.asarray(embeddings, dtype="float32")

    def build_vector_index(self, vectors: np.ndarray) -> faiss.Index:
        """Build an in-memory FAISS index from all chunk vectors."""
        if vectors.ndim != 2 or vectors.shape[0] == 0:
            raise ValueError("Expected a non-empty 2D array of chunk vectors")

        dimension = vectors.shape[1]
        index = faiss.IndexFlatL2(dimension)
        index.add(vectors)
        return index

    def save_vector_index(self, index: faiss.Index, path: Path | None = None) -> Path:
        """Persist the FAISS index to disk."""
        target_path = path or self._vector_index_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        faiss.write_index(index, str(target_path))
        return target_path

    def _embedding_model_instance(self) -> SentenceTransformer:
        if self._embedding_model is None:
            self._embedding_model = SentenceTransformer(settings.rag_embedding_model_name)
        return self._embedding_model

    @staticmethod
    def _parent_classes_by_uri(classes: list[dict]) -> dict[str, list[str]]:
        parent_map: dict[str, list[str]] = {}
        for class_data in classes:
            class_uri = class_data.get("uri")
            if not isinstance(class_uri, str) or not class_uri:
                continue
            parent_map[class_uri] = [
                parent_uri
                for parent_uri in class_data.get("parent_classes", [])
                if isinstance(parent_uri, str) and parent_uri
            ]
        return parent_map

    @classmethod
    def _properties_for_class_and_parents(
        cls,
        properties: list[dict],
        class_uri: str | None,
        parent_classes_by_uri: dict[str, list[str]],
    ) -> list[str]:
        if not class_uri:
            return []

        collected_properties: set[str] = set()
        visited: set[str] = set()
        for related_class_uri in cls._class_lineage(class_uri, parent_classes_by_uri, visited):
            collected_properties.update(cls._properties_for_class(properties, related_class_uri))
        return sorted(collected_properties)

    @staticmethod
    def _class_lineage(
        class_uri: str,
        parent_classes_by_uri: dict[str, list[str]],
        visited: set[str],
    ) -> list[str]:
        if class_uri in visited:
            return []

        visited.add(class_uri)
        lineage = [class_uri]
        for parent_uri in parent_classes_by_uri.get(class_uri, []):
            lineage.extend(RAGIndexService._class_lineage(parent_uri, parent_classes_by_uri, visited))
        return lineage

    @staticmethod
    def _properties_for_class(properties: list[dict], class_uri: str) -> list[str]:
        if not class_uri:
            return []

        related = []
        for property_data in properties:
            domains = property_data.get("domain", [])
            if class_uri not in domains:
                continue
            property_name = (
                property_data.get("name")
                or property_data.get("label")
                or property_data.get("uri")
            )
            if not property_name:
                continue
            ranges = property_data.get("range", [])
            range_name = "Unknown"
            if isinstance(ranges, list) and ranges:
                first_range = ranges[0]
                if first_range:
                    range_name = RAGIndexService._short_name(first_range)
            related.append(f"{RAGIndexService._short_name(property_name)} -> {range_name}")
        return sorted(set(related))

    @staticmethod
    def _build_chunk_text(
        class_name: str,
        class_label: str | None,
        description: str,
        parent_classes: list[str],
        object_properties: list[str],
        datatype_properties: list[str],
    ) -> str:
        parent_text = RAGIndexService._bullet_list(parent_classes, empty_label="None (top-level class)")
        object_property_text = RAGIndexService._bullet_list(object_properties)
        datatype_property_text = RAGIndexService._bullet_list(datatype_properties)
        description_text = description or "No description available."
        label_text = class_label or "No label available."

        return (
            f"Class: {class_name}\n\n"
            f"Label: {label_text}\n\n"
            f"Description: {description_text}\n\n"
            f"Parent Classes:\n{parent_text}\n\n"
            f"Object Properties:\n{object_property_text}\n\n"
            f"Datatype Properties:\n{datatype_property_text}"
        )

    @staticmethod
    def _description_for(class_data: dict[str, object], class_name: str) -> str:
        description = (class_data.get("comment") or "").strip()
        if description:
            return description

        label = class_data.get("label")
        if label:
            label = label.strip()
            if label and label != class_name:
                return label

        return "No description available"

    @staticmethod
    def _short_name(value: str) -> str:
        if "#" in value:
            return value.rsplit("#", 1)[-1]
        if "/" in value:
            return value.rstrip("/").rsplit("/", 1)[-1]
        if ":" in value:
            return value.rsplit(":", 1)[-1]
        return value

    @staticmethod
    def _bullet_list(values: list[str], empty_label: str = "None") -> str:
        if not values:
            return f"- {empty_label}"
        return "\n".join(f"- {value}" for value in values)
