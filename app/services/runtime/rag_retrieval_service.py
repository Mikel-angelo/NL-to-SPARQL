"""Retrieve ontology class chunks relevant to a natural-language question."""

from pathlib import Path
import json

import faiss
import numpy as np
from fastapi import HTTPException, status
from sentence_transformers import SentenceTransformer

from app.core.config import settings


class RAGRetrievalService:
    """Load the persisted RAG artifacts and retrieve the closest class chunks."""

    def __init__(self, storage_dir: Path | None = None) -> None:
        self._storage_dir = storage_dir or Path(settings.storage_path)
        self._current_dir = self._storage_dir / "current"
        self._class_chunks_path = self._current_dir / "class_chunks.json"
        self._vector_index_path = self._current_dir / "index.faiss"
        self._embedding_model: SentenceTransformer | None = None

    def retrieve(self, question: str) -> list[dict[str, object]]:
        """Return the top matching persisted class chunks for the incoming question."""
        class_chunks = self._load_class_chunks()
        index = self._load_vector_index()
        if index.ntotal == 0:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="The runtime vector index is empty",
            )

        query_vector = self._embed_question(question)
        k = min(settings.runtime_retrieval_top_k, index.ntotal)
        distances, indices = index.search(query_vector, k=k)

        results: list[dict[str, object]] = []
        for rank, chunk_index in enumerate(indices[0], start=1):
            if chunk_index < 0 or chunk_index >= len(class_chunks):
                continue
            chunk = class_chunks[chunk_index]
            results.append(
                {
                    "rank": rank,
                    "score": float(distances[0][rank - 1]),
                    "class_name": chunk.get("class_name"),
                    "class_uri": chunk.get("class_uri"),
                    "text": chunk.get("text"),
                    "metadata": chunk.get("metadata"),
                }
            )
        return results

    def _load_class_chunks(self) -> list[dict[str, object]]:
        if not self._class_chunks_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Runtime artifact not found: {self._class_chunks_path.as_posix()}",
            )

        try:
            content = self._class_chunks_path.read_text(encoding="utf-8")
            class_chunks = json.loads(content)
        except (OSError, json.JSONDecodeError) as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to load runtime class chunks",
            ) from exc

        if not isinstance(class_chunks, list):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Runtime class chunks have an invalid format",
            )
        return class_chunks

    def _load_vector_index(self) -> faiss.Index:
        if not self._vector_index_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Runtime artifact not found: {self._vector_index_path.as_posix()}",
            )

        try:
            return faiss.read_index(str(self._vector_index_path))
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to load the runtime vector index",
            ) from exc

    def _embed_question(self, question: str) -> np.ndarray:
        embeddings = self._embedding_model_instance().encode(
            [question],
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        return np.asarray(embeddings, dtype="float32")

    def _embedding_model_instance(self) -> SentenceTransformer:
        if self._embedding_model is None:
            self._embedding_model = SentenceTransformer(settings.rag_embedding_model_name)
        return self._embedding_model
