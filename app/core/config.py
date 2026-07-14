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
    ontology_packages_path: str = "ontology_packages"

    rag_embedding_model_name: str = "all-MiniLM-L6-v2"
    runtime_retrieval_top_k: int = 5
    runtime_abox_retrieval_top_k: int = 5
    default_chunking_strategy: str = "class_based"
    default_build_abox_index: bool = True
    default_use_abox_rag: bool = False
    default_use_reactive_abox_discovery: bool = False
    llm_api_url: str = "http://147.102.6.253:11500/api/generate"
    default_llm_model: str = "qwen2.5-coder:32b"
    llm_timeout_seconds: float = 120.0
    llm_temperature: float = 0.0
    llm_num_ctx: int = 32768
    correction_max_iterations: int = 3
    # Output token cap. Set high enough for reasoning models (e.g. DeepSeek-R1)
    # to emit their full <think> block plus the query; the normalizer strips the
    # reasoning afterward. Applied uniformly to all models.
    llm_num_predict: int = 8192


settings = Settings()
