# NL-to-SPARQL

FastAPI service for ontology onboarding into Apache Jena Fuseki, with local storage for one current ontology and its extracted runtime context.

The repository name is `NL-to-SPARQL`, but the currently implemented API focuses on ontology loading, schema resolution, context extraction, and Fuseki dataset management. Natural-language-to-SPARQL runtime endpoints are not implemented yet.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Prerequisites

- Python 3.11+ is recommended
- Apache Jena Fuseki must be running at `http://127.0.0.1:3030`
- outbound HTTP access is required if you want automatic external schema resolution for missing class namespaces

## Run

Start Fuseki first:

```powershell
docker compose -f infra/docker/compose.yml up -d
```

Then start the API:

```powershell
uvicorn app.main:app --reload
```

Docs are available at `http://127.0.0.1:8000/docs`.

## Current Model

The framework keeps exactly one current ontology locally and exactly one current ontology dataset in Fuseki.

When you load a new ontology:
- the uploaded file is parsed first with RDFLib
- a fast detection step classifies the file as `schema-only`, `mixed`, or `instances-only`
- a schema-coverage step checks whether instance `rdf:type` class URIs are declared locally
- missing class namespaces trigger heuristic schema resolution
- a final in-memory graph is built from the uploaded ontology plus any resolved schemas
- runtime metadata is saved as `storage/current/metadata.json`
- parsed ontology structure is saved as `storage/current/ontology_context.json`
- step-by-step onboarding logs are saved as `storage/current/load.log`
- the previous Fuseki dataset is removed after the new dataset is created and all files are uploaded

The current storage contains:
- `storage/current/ontology.ttl` or `storage/current/ontology.owl` or `storage/current/ontology.rdf`
- `storage/current/schemas/` when external schemas were resolved
- `storage/current/metadata.json`
- `storage/current/ontology_context.json`
- `storage/current/load.log`

## API

### `GET /`

Serves a simple static HTML page for:
- uploading one ontology file to the existing onboarding endpoint
- viewing the current `load.log`
- viewing the current `metadata.json`

### `GET /load-log`

Returns the current onboarding log as plain text.

### `GET /metadata`

Returns the current runtime metadata JSON as plain text.

### `GET /health`

Returns a simple service health payload:

```json
{"status": "ok"}
```

### `POST /ontology/load`

Loads an ontology file into the framework and Fuseki.

Notes:
- Fuseki must already be running, otherwise the upload will fail during dataset replacement
- schema resolution may issue outbound HTTP requests when instance class namespaces are missing from the uploaded ontology

Accepted formats:
- `.ttl`
- `.owl`
- `.rdf`

## Service Split

`OntologyOnboardingService`
- validate the uploaded file
- build dataset naming from filename + timestamp
- parse the initial graph
- run fast detection and structural mode classification
- analyze schema coverage for instance type URIs
- resolve schemas for missing class namespaces when possible
- build the final graph
- save the current ontology file, metadata, ontology context, and load log
- replace the current Fuseki dataset and upload all loaded files

`OntologySchemaResolutionService`
- parse the initial ontology graph
- run fast detection counts
- classify ontology mode
- analyze schema coverage
- resolve external schemas heuristically for missing class namespaces
- build the final graph

`OntologyContextService`
- parse classes
- parse object properties
- parse datatype properties
- parse labels and comments
- parse class hierarchy
- parse prefixes
- collect instance-level statistics
- build `ontology_context.json`

`FusekiService`
- create dataset
- delete dataset
- replace dataset
- upload ontology RDF
- execute SPARQL query against a dataset

## Onboarding Flow

1. `OntologyOnboardingService` validates the upload, normalizes the ontology name, and prepares the target Fuseki dataset name.
2. `OntologySchemaResolutionService.parse_uploaded_content()` parses the uploaded bytes into the `initial_graph`.
3. `OntologySchemaResolutionService.detect()` counts classes, properties, and instances for a coarse structural view of the file.
4. `OntologySchemaResolutionService.classify_mode()` labels the file as `schema-only`, `mixed`, or `instances-only`. This is descriptive metadata, not the main resolution gate.
5. `OntologySchemaResolutionService.analyze_schema_coverage()` compares instance `rdf:type` class URIs with locally declared classes and marks coverage as `complete` or `incomplete`.
6. If coverage is incomplete, `OntologySchemaResolutionService.resolve_schemas_for_namespaces()` tries to download RDF schemas for the missing class namespaces.
7. `OntologySchemaResolutionService.build_final_graph()` merges the uploaded graph with any resolved schemas into the `final_graph`.
8. `OntologyContextService.extract_context()` reads the `final_graph` and builds the normalized `ontology_context.json` payload.
9. `OntologyOnboardingService` writes the current ontology file, optional schema files, `metadata.json`, `ontology_context.json`, and `load.log` under `storage/current`.
10. `FusekiService.replace_dataset()` creates the new Fuseki dataset, uploads the original ontology file plus any resolved schema files, and then removes the previous dataset.

## Fuseki With Docker Compose

Compose files live under `infra/docker`.

Start Fuseki:

```powershell
docker compose -f infra/docker/compose.yml up -d
```

Stop Fuseki:

```powershell
docker compose -f infra/docker/compose.yml down
```

Fuseki UI is available at `http://127.0.0.1:3030`.
Fuseki data is stored at the project root in `fuseki-data`.

Admin login:
- username: `admin`
- password: `admin`
