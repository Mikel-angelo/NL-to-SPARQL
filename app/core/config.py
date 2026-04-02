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


settings = Settings()
