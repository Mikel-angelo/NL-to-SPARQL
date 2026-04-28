# NL-to-SPARQL

This project turns an ontology into a reusable local package, then uses that package to answer natural-language questions by generating and running SPARQL.

The main workflow is:

1. Onboard an ontology into `ontology_packages/`.
2. Query the generated package with `query.py`.
3. Optionally use the FastAPI routes, which call the same underlying code.

## Requirements

- Python 3.11+
- Docker, if you want to run the bundled Fuseki server
- Apache Jena Fuseki available at `http://127.0.0.1:3030`
- Access to the configured LLM API for query generation
- Internet access only if onboarding needs to resolve missing external schemas

Default runtime settings live in `app/core/config.py`:

- Fuseki: `http://127.0.0.1:3030`
- Fuseki admin login: `admin` / `admin`
- Embedding model: `all-MiniLM-L6-v2`
- Default LLM model: `qwen2.5-coder:7b`
- LLM API URL: `http://147.102.6.253:11500/api/generate`

## Setup

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Start Fuseki:

```powershell
docker compose -f infra/docker/compose.yml up -d
```

Fuseki UI:

```text
http://127.0.0.1:3030
```

## CLI Usage

There are two CLI commands:

- `onboard.py`: creates an ontology package
- `query.py`: queries an existing ontology package

Both commands require explicit arguments. If a required argument is missing, Python argparse stops immediately, prints a usage error, and exits without running the pipeline.

### `onboard.py`

Use `onboard.py` when you want to prepare an ontology for querying.

Required arguments:

- one source argument:
  - `--ontology path\to\file.ttl`
  - or `--sparql-endpoint http://.../query`
- `--output ontology_packages`

You must provide exactly one source. Do not pass both `--ontology` and `--sparql-endpoint`.

Basic file onboarding:

```powershell
.\.venv\Scripts\python.exe onboard.py --ontology resources\library\ontologies\eNOVATION.ttl --output ontology_packages
```

Accepted ontology formats:

- `.ttl`
- `.owl`
- `.rdf`

During onboarding, the CLI:

- parses the ontology
- resolves missing schemas when possible
- extracts a normalized ontology context
- creates retrieval chunks
- builds a FAISS index
- uploads the ontology data to Fuseki
- creates a new package under `ontology_packages/`
- marks that package as active for the API

At the end, the command prints values like:

```text
Ontology package: C:\...\ontology_packages\enovation-20260427-184012-632861
Dataset name: enovation-20260427-184012-632861
Dataset endpoint: http://127.0.0.1:3030/enovation-20260427-184012-632861
Query endpoint: http://127.0.0.1:3030/enovation-20260427-184012-632861/query
Artifacts: ...\chunks.json | ...\index.faiss
```

Use the printed `Ontology package` path in the query command.

Onboard an existing SPARQL endpoint instead of a local file:

```powershell
.\.venv\Scripts\python.exe onboard.py --sparql-endpoint http://127.0.0.1:3030/my-dataset/query --output ontology_packages
```

This creates the same package structure, but does not upload a new local ontology file to Fuseki.

Optional onboarding arguments:

- `--model`: save a different default LLM model in `settings.json`
- `--chunking`: save a different chunking strategy name; defaults to `class_based`

Supported chunking strategies:

- `class_based`: one chunk per class with class label, description, and direct properties with ranges
- `property_based`: one chunk per property with property label, description, domain classes, and range classes or datatypes
- `composite`: one chunk per class neighbourhood with the class, direct properties, parent classes, and child classes

Example:

```powershell
.\.venv\Scripts\python.exe onboard.py --ontology path\to\ontology.ttl --output ontology_packages --model qwen2.5-coder:7b
```

### `query.py`

Use `query.py` when you already have an ontology package and want to ask a natural-language question.

Required arguments:

- `--question "your question"`

Optional package argument:

- `--package path\to\ontology-package`

If you omit `--package`, the CLI uses the active package stored in:

```text
ontology_packages/.active_package
```

That file is updated automatically after successful onboarding. If no active package is set, the command fails with:

```text
No active ontology package is set
```

Basic query using the active package:

```powershell
.\.venv\Scripts\python.exe query.py --question "Which training centres offer CBRN exercises?"
```

Query a specific package:

```powershell
.\.venv\Scripts\python.exe query.py --package ontology_packages\enovation-20260427-184012-632861 --question "Which training centres offer CBRN exercises?"
```

The query command:

- loads the package artifacts
- retrieves the most relevant ontology chunks
- generates SPARQL with the configured LLM
- validates the SPARQL through formal validation stages
- retries failed validation/execution through the LLM self-correction loop
- runs the final SPARQL against the package query endpoint
- writes a trace to `logs/query.log`

The output includes:

- `Answer`: raw SPARQL execution result
- `Generated SPARQL`: the generated query
- `Trace`: path to the query log
- `Status`: pipeline status
- `Errors`: validation or execution errors, if any

The self-correction loop is controlled by:

```text
settings.correction_max_iterations = 3
```

Each query trace records the original generated query, every correction iteration, validation stage results, execution result, final query, and final status.

Optional query arguments:

- `--model`: use a different LLM model for this query only
- `--endpoint`: use a different SPARQL query endpoint
- `--k`: change how many chunks are retrieved

Example with query overrides:

```powershell
.\.venv\Scripts\python.exe query.py --package ontology_packages\my-package --question "..." --model qwen2.5-coder:7b --k 5
```

## Package Layout

Each onboarding run creates a self-contained package:

```text
ontology_packages/
  <ontology-name>-<timestamp>/
    metadata.json
    ontology_context.json
    settings.json
    ontology/
      source.ttl
      schemas/
    chunks/
      chunks.json
      index.faiss
    logs/
      onboard.log
      query.log
```

Important files:

- `metadata.json`: onboarding summary and artifact paths
- `settings.json`: saved endpoint, model, and selected chunking strategy
- `ontology_context.json`: normalized ontology structure used by the runtime
- `chunks/chunks.json`: text chunks used for retrieval
- `chunks/index.faiss`: vector index for retrieval
- `logs/onboard.log`: onboarding trace
- `logs/query.log`: query trace

The active package path is stored in:

```text
ontology_packages/.active_package
```

Both the CLI query command and the FastAPI routes use this active package when no explicit package is provided.

## FastAPI Usage

Start the API:

```powershell
uvicorn app.main:app --reload
```

Open the API docs:

```text
http://127.0.0.1:8000/docs
```

Main routes:

- `GET /health`: service health check
- `GET /metadata`: metadata for the active package
- `GET /load-log`: onboarding log for the active package
- `GET /query-pipeline-log`: query log for the active package
- `POST /ontology/load`: upload and onboard an ontology file
- `POST /query`: query the active package

`POST /ontology/load` accepts multipart form data:

- `file`: ontology file, required
- `chunking`: optional; one of `class_based`, `property_based`, or `composite`; defaults to `class_based`

## RAG Module API

The indexing and retrieval logic is available without running the full query pipeline:

```python
from app.domain.rag import build_index, retrieve_context, retrieve_text_chunks

build_index("ontology_packages/my-package", chunking="composite")

chunks = retrieve_context(
    "ontology_packages/my-package",
    "Which training centres offer CBRN exercises?",
    k=5,
)

texts = retrieve_text_chunks(
    "ontology_packages/my-package",
    "Which training centres offer CBRN exercises?",
    k=5,
)
```

## Code Structure

```text
app/
  api/routes/          HTTP routes
  clients/             external clients such as Fuseki and LLM calls
  core/config.py       default settings
  domain/package.py    package discovery and active-package helpers
  domain/ontology/
    onboard_pipeline.py        full onboarding pipeline used by CLI and API
    onboarding_extraction.py   source parsing, schema resolution, package JSON writing
    context_builder.py         RDFLib graph -> ontology_context.json structure
    schema_resolution.py       RDF parsing and schema coverage/resolution helpers
  domain/rag/
    chunking.py                chunk construction strategies
    build_index.py             chunks/chunks.json and chunks/index.faiss building
    retrieve_context.py        semantic chunk retrieval from a package
  domain/runtime/      SPARQL prompt generation, validation, self-correction, execution
    pipeline.py                 runtime query pipeline orchestration
    prompt_renderer.py          Jinja2 prompt rendering
    validation.py               formal SPARQL validation stages
    correction_loop.py          LLM self-correction loop
    templates/
      query_generation_prompt.j2
      query_correction_prompt.j2
onboard.py             CLI wrapper for onboarding
query.py               query CLI
ontology_packages/     generated packages
```

## Notes

- Each file-based onboarding run creates a new Fuseki dataset.
- After a successful file-based onboarding run, the previous active Fuseki dataset is removed.
- Package directories are timestamped, so repeated runs do not overwrite older packages.
- If query generation fails, check that the configured LLM API URL and model are reachable.
