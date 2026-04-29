# NL-to-SPARQL

This project turns an ontology into a reusable local package, then uses that package to answer natural-language questions by generating and running SPARQL.

The main workflow is:

1. Onboard an ontology into `ontology_packages/`.
2. Activate the package when you want it loaded into the managed Fuseki instance.
3. Query the active package with `query.py`.
4. Optionally evaluate the active package with `evaluate.py`.
5. Optionally use the FastAPI routes, which call the same underlying code.

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

There are four main CLI commands:

- `onboard.py`: creates an ontology package
- `activate.py`: makes an existing package active for runtime querying
- `query.py`: queries the active ontology package
- `evaluate.py`: runs an evaluation dataset against the active package

These commands require explicit arguments where noted. If a required argument is missing, Python argparse stops immediately, prints a usage error, and exits without running the pipeline.

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

During file onboarding, the CLI:

- parses the ontology
- resolves missing schemas when possible
- extracts a normalized ontology context
- creates retrieval chunks
- builds a FAISS index
- uploads the ontology data to Fuseki
- creates a new package under `ontology_packages/`
- marks that package as active for the CLI and API

At the end, the command prints values like:

```text
Ontology package: C:\...\ontology_packages\enovation-20260427-1840
Dataset name: enovation-20260427-1840
Dataset endpoint: http://127.0.0.1:3030/enovation-20260427-1840
Query endpoint: http://127.0.0.1:3030/enovation-20260427-1840/query
Artifacts: ...\chunks.json | ...\index.faiss
```

Use the printed `Ontology package` path with `activate.py` if you later need to switch back to this package.

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

### `activate.py`

Use `activate.py` when you want an existing package to become the active runtime package.

```powershell
.\.venv\Scripts\python.exe activate.py --package ontology_packages\enovation-20260427-1840
```

For file-based packages, activation always reloads the package into Fuseki:

- deletes any existing Fuseki dataset with the package dataset name
- recreates that dataset
- uploads `ontology/source.*`
- uploads files from `ontology/schemas/`
- writes `ontology_packages/.active_package`
- removes the previously active local Fuseki dataset when it is different

For SPARQL-endpoint packages, activation only marks the package active. The endpoint is externally managed, so this project does not upload or recreate that dataset.

### `query.py`

Use `query.py` when the ontology package you want to query is already active. There is one supported query path: activate the package first, then query the active package.

For local file packages, "active" means two things:

- `ontology_packages/.active_package` points at the package directory
- the package's dataset has been loaded into the managed Fuseki server

Run `activate.py` before querying an older local package. `query.py` has no package selector. It always uses `ontology_packages/.active_package`.

Required arguments:

- `--question "your question"`

The CLI uses the active package stored in:

```text
ontology_packages/.active_package
```

That file is updated automatically after successful onboarding and activation. If no active package is set, the command fails with:

```text
No active ontology package is set
```

Safe query flow for a local file package:

```powershell
.\.venv\Scripts\python.exe activate.py --package ontology_packages\enovation-20260427-1840
.\.venv\Scripts\python.exe query.py --question "Which training centres offer CBRN exercises?"
```

Query using the currently active package:

```powershell
.\.venv\Scripts\python.exe query.py --question "Which training centres offer CBRN exercises?"
```

The query command:

- loads the package artifacts
- retrieves the most relevant ontology chunks
- generates SPARQL with the configured LLM
- validates each candidate query through formal validation stages
- executes a candidate only after validation passes
- asks the LLM for a corrected candidate when validation or execution fails
- writes a machine trace to `logs/query.log`
- writes readable text traces to `logs/query-latest.txt` and `logs/query-runs/`

The output includes:

- `Answer`: raw SPARQL execution result
- `Generated SPARQL`: the generated query
- `Trace`: path to the query log
- `Readable trace`: path to the plain-text query trace
- `Status`: pipeline status
- `Errors`: validation or execution errors, if any

The runtime attempt loop is controlled by:

```text
settings.correction_max_iterations = 3
```

Each JSON query trace records the original generated query, every correction iteration, validation stage results, execution result, final query, and final status. Each iteration also includes scan-friendly fields: `status`, `validation_summary`, and `errors`.

For debugging prompts and generated SPARQL, prefer the readable text trace:

```text
logs/query-latest.txt
logs/query-runs/<run-id>.txt
```

Optional query arguments:

- `--model`: use a different LLM model for this query only
- `--k`: change the retrieval top-k, meaning how many ontology chunks are retrieved for the prompt

Example with query overrides:

```powershell
.\.venv\Scripts\python.exe query.py --question "..." --model qwen2.5-coder:7b --k 5
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
      query-latest.txt
      query-runs/
```

Important files:

- `metadata.json`: onboarding summary and artifact paths
- `settings.json`: saved endpoint, model, and selected chunking strategy
- `ontology_context.json`: normalized ontology structure used by the runtime
- `chunks/chunks.json`: text chunks used for retrieval
- `chunks/index.faiss`: vector index for retrieval
- `logs/onboard.log`: onboarding trace
- `logs/query.log`: machine-readable query trace JSON
- `logs/query-latest.txt`: latest human-readable query trace
- `logs/query-runs/`: timestamped human-readable query traces

The active package path is stored in:

```text
ontology_packages/.active_package
```

The CLI query command and FastAPI routes use this active package. For local file packages, this is the only runtime path because activation is what reloads Fuseki.

## Evaluation

Use `evaluate.py` to run a dataset of natural-language questions and gold SPARQL answers against one package.

```powershell
.\.venv\Scripts\python.exe activate.py --package ontology_packages\enovation-20260427-1840
.\.venv\Scripts\python.exe evaluate.py --dataset evaluation\datasets\enovation_v1.json --package ontology_packages\enovation-20260427-1840
```

Evaluation calls the runtime pipeline directly, not the HTTP API. This keeps query latency focused on retrieval, generation, validation, correction, and SPARQL execution rather than FastAPI transport overhead.

Important behavior:

- the requested package must already be the active package
- evaluation does not activate or reload packages automatically
- the configured query endpoint is checked once before timed question execution starts
- outputs are written under `<package>/evaluation/<run-id>/` by default
- questions with empty `gold_answers` are run but marked `missing_gold` / unscored
- unscored questions count toward latency, validation, execution, and correction metrics, but not correctness metrics
- `--k` is retrieval top-k, not the correction iteration count

Evaluation output files:

- `index.txt`: one-line status summary for every question
- `results.json`: per-question pipeline output, answers, traces, and scoring status
- `metrics.json`: aggregate metrics
- `report.txt`: readable summary
- `queries.jsonl`: compact machine-readable one-record-per-question log
- `queries/Qxxx.txt`: readable per-question debugging files with gold query, final query, answers, diff, and trace paths

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

The API has no package selector for `/query`. It always queries the active package. For local file packages, activate the package first so Fuseki is loaded with the matching dataset.

`POST /ontology/load` accepts multipart form data:

- `file`: ontology file, required
- `chunking`: optional; one of `class_based`, `property_based`, or `composite`; defaults to `class_based`

The static UI at `GET /` exposes the same upload route and includes a chunking strategy selector.

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
    package_activation.py      package activation and Fuseki reload behavior
    onboarding_workflow.py     top-level onboarding workflow used by CLI and API
    source_loader.py           local file or SPARQL endpoint -> RDFLib graph
    graph_preparation.py       graph detection, schema resolution, and FinalGraph creation
    ontology_context.py        RDFLib graph -> ontology_context.json structure
    package_writer.py          metadata/settings/context/source/schema artifact writing
  domain/rag/
    chunking.py                chunk construction strategies
    build_index.py             chunks/chunks.json and chunks/index.faiss building
    retrieve_context.py        semantic chunk retrieval from a package
  domain/runtime/      SPARQL prompt generation, validation, self-correction, execution
    pipeline.py                 runtime query pipeline orchestration
    query_generation.py          initial LLM query generation and output normalization
    query_correction.py          correction prompt rendering plus corrected-query generation
    sparql_execution.py          SPARQL endpoint execution
    prompt_renderer.py          Jinja2 prompt rendering
    validation.py               formal SPARQL validation stages
    templates/
      query_generation_prompt.j2
      query_correction_prompt.j2
activate.py            package activation CLI
evaluate.py            direct package evaluation CLI
onboard.py             onboarding CLI
query.py               query CLI
evaluation/            evaluation datasets, runner, answer comparison, and metrics
ontology_packages/     generated packages
```

## Notes

- Each file-based onboarding run creates a new package and Fuseki dataset.
- File package activation recreates that package's Fuseki dataset from package artifacts.
- After successful file onboarding or activation, the previous active local Fuseki dataset is removed.
- `query.py` never accepts a package path or endpoint override. Activation is the operation that chooses the package and guarantees the managed Fuseki dataset matches it.
- Package directories are timestamped, so repeated runs do not overwrite older packages.
- If query generation fails, check that the configured LLM API URL and model are reachable.


