import json
from pathlib import Path

import faiss
from sentence_transformers import SentenceTransformer


def main() -> None:
    chunks_path = Path("storage/current/class_chunks.json")
    index_path = Path("storage/current/index.faiss")

    if not chunks_path.exists():
        print(f"Chunks file not found: {chunks_path.resolve()}")
        raise SystemExit(1)

    if not index_path.exists():
        print(f"FAISS file not found: {index_path.resolve()}")
        raise SystemExit(1)

    with chunks_path.open("r", encoding="utf-8") as file:
        chunks = json.load(file)

    index = faiss.read_index(str(index_path))

    print("FAISS index loaded successfully.")
    print("Path:", index_path.resolve())
    print("Entries:", index.ntotal)
    print("Dimension:", index.d)

    model = SentenceTransformer("all-MiniLM-L6-v2")

    query = "What types of actors exist in scenarios?"
    query_vector = model.encode([query], normalize_embeddings=True)

    distances, indices = index.search(query_vector, k=3)

    print(f"Query: {query}\n")
    print("Top 3 results:\n")

    for rank, idx in enumerate(indices[0], start=1):
        chunk = chunks[idx]
        print(f"Result {rank}")
        print("Class:", chunk.get("class_name"))
        print("URI:", chunk.get("class_uri"))
        print("Text:")
        print(chunk.get("text"))
        print("-" * 60)


if __name__ == "__main__":
    main()
