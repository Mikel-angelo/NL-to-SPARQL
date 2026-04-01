from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = "NL-to-SPARQL API"
    app_version: str = "0.1.0"

    fuseki_base_url: str = "http://localhost:3030"
    fuseki_admin_username: str = "admin"
    fuseki_admin_password: str = "admin"
    ontology_storage_path: str = "resources/ontologies"


settings = Settings()
