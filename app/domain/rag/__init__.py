"""RAG artifact building and retrieval APIs."""

from app.domain.rag.build_index import IndexBuildResult, build_index
from app.domain.rag.chunking import SUPPORTED_CHUNKING_STRATEGIES, build_chunks
from app.domain.rag.retrieve_context import RetrievedChunk, retrieve_context, retrieve_text_chunks

__all__ = [
    "IndexBuildResult",
    "RetrievedChunk",
    "SUPPORTED_CHUNKING_STRATEGIES",
    "build_index",
    "build_chunks",
    "retrieve_context",
    "retrieve_text_chunks",
]
