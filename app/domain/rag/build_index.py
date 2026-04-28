"""Build RAG retrieval artifacts for an ontology package.

This module builds `chunks/chunks.json` and `chunks/index.faiss` from an
existing `ontology_context.json`. Onboarding calls it after extraction, and it
can also be called directly by API/CLI surfaces that only rebuild retrieval.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

from app.core.config import settings
from app.domain.package import (
    PackageNotFoundError,
    chunks_path,
    index_path,
    metadata_path,
    read_json_file,
    resolve_package_dir,
    chunks_dir,
    settings_path,
    write_json_file,
    ontology_context_path,
)
from app.domain.rag.chunking import build_chunks


_EMBEDDING_MODEL: SentenceTransformer | None = None


@dataclass(frozen=True)
class IndexBuildResult:
    """Artifacts produced for one package retrieval index."""

    package_dir: Path
    chunking: str
    chunks_path: Path
    index_path: Path
    chunk_count: int
    embedding_model: str


def build_index(
    package_dir: str | Path,
    *,
    chunking: str = "class_based",
) -> IndexBuildResult:
    """Build chunks and vector index for one ontology package."""
    root = resolve_package_dir(package_dir)
    ontology_context = read_json_file(ontology_context_path(root))
    metadata = read_json_file(metadata_path(root))

    chunks = build_chunks(ontology_context, chunking)
    if not chunks:
        raise PackageNotFoundError("No chunks could be built from ontology_context.json")

    texts = [str(chunk["text"]) for chunk in chunks]
    vectors = embed_texts(texts)
    index = build_vector_index(vectors)

    chunk_dir = chunks_dir(root)
    chunk_dir.mkdir(parents=True, exist_ok=True)
    chunks_file = chunks_path(root)
    chunks_file.write_text(json.dumps(chunks, indent=2), encoding="utf-8")
    index_file = index_path(root)
    faiss.write_index(index, str(index_file))

    files_loaded = metadata.setdefault("files_loaded", [])
    if isinstance(files_loaded, list):
        relative_chunks = chunks_file.relative_to(root).as_posix()
        relative_index = index_file.relative_to(root).as_posix()
        if relative_chunks not in files_loaded:
            files_loaded.append(relative_chunks)
        if relative_index not in files_loaded:
            files_loaded.append(relative_index)
    metadata["runtime_artifacts"] = {
        "chunking_strategy": chunking,
        "chunks_file": chunks_file.relative_to(root).as_posix(),
        "index_file": index_file.relative_to(root).as_posix(),
        "count": len(chunks),
        "embedding_model": settings.rag_embedding_model_name,
    }
    write_json_file(metadata_path(root), metadata)

    settings_payload = read_json_file(settings_path(root))
    settings_payload["chunking_strategy"] = chunking
    write_json_file(settings_path(root), settings_payload)

    return IndexBuildResult(
        package_dir=root,
        chunking=chunking,
        chunks_path=chunks_file,
        index_path=index_file,
        chunk_count=len(chunks),
        embedding_model=settings.rag_embedding_model_name,
    )


def embed_texts(texts: list[str]) -> np.ndarray:
    """Embed chunk or query text with the configured sentence-transformer model."""
    embeddings = embedding_model().encode(
        texts,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    return np.asarray(embeddings, dtype="float32")


def build_vector_index(vectors: np.ndarray) -> faiss.Index:
    """Build a simple flat FAISS index."""
    if vectors.ndim != 2 or vectors.shape[0] == 0:
        raise ValueError("Expected a non-empty 2D array of vectors")
    index = faiss.IndexFlatL2(vectors.shape[1])
    index.add(vectors)
    return index


def embedding_model() -> SentenceTransformer:
    """Return a cached sentence-transformer instance."""
    global _EMBEDDING_MODEL
    if _EMBEDDING_MODEL is None:
        _EMBEDDING_MODEL = SentenceTransformer(settings.rag_embedding_model_name)
    return _EMBEDDING_MODEL
