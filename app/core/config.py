from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = "NL-to-SPARQL API"
    app_version: str = "0.1.0"

    fuseki_base_url: str = "http://127.0.0.1:3030"
    fuseki_admin_username: str = "admin"
    fuseki_admin_password: str = "admin"
    fuseki_admin_timeout_seconds: float = 180.0
    fuseki_upload_timeout_seconds: float = 600.0
    storage_path: str = "storage"

    rag_embedding_model_name: str = "all-MiniLM-L6-v2"
    runtime_retrieval_top_k: int = 10
    ollama_url: str = "http://147.102.6.253:11500/api/generate"
    ollama_model: str = "qwen2.5-coder:7b"
    llm_timeout_seconds: float = 30.0
    llm_temperature: float = 0.0
    llm_num_ctx: int = 4096


settings = Settings()
