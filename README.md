# NL-to-SPARQL

FastAPI service for loading one ontology into Apache Jena Fuseki and storing one current ontology locally.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run

```powershell
uvicorn app.main:app --reload
```

Docs are available at `http://127.0.0.1:8000/docs`.

## Current Model

The framework keeps exactly one current ontology locally and exactly one current ontology dataset in Fuseki.

When you load a new ontology:
- the uploaded file is parsed first with RDFLib
- a fast detection step classifies the file as `schema-only`, `mixed`, or `instances-only`
- `instances-only` files trigger heuristic schema resolution
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

### `POST /ontology/load`

Loads an ontology file into the framework and Fuseki.

Accepted formats:
- `.ttl`
- `.owl`
- `.rdf`

## Service Split

`OntologyOnboardingService`
- validate the uploaded file
- build dataset naming from filename + timestamp
- parse the initial graph
- run fast detection and mode classification
- resolve schemas for `instances-only` inputs when possible
- build the final graph
- save the current ontology file, metadata, ontology context, and load log
- replace the current Fuseki dataset and upload all loaded files

`OntologySchemaResolutionService`
- parse the initial ontology graph
- run fast detection counts
- classify ontology mode
- resolve external schemas heuristically
- build the final graph

`OntologyContextService`
- parse classes
- parse object properties
- parse datatype properties
- parse labels and comments
- parse class hierarchy
- parse prefixes
- optionally collect instance-level statistics
- build `ontology_context.json`

`FusekiService`
- create dataset
- delete dataset
- replace dataset
- upload ontology RDF

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
