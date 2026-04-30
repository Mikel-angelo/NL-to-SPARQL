# Query Flow

Querying starts from the active package, retrieves ontology context, asks the LLM for SPARQL, validates it, executes it, and optionally asks for corrections.

```mermaid
flowchart TD
    cli[query.py\nor POST /query]
    active[get_active_package]
    pipeline[run_query_pipeline]
    package[read metadata/settings/context]
    retrieve[retrieve_context]
    prompt[render_query_generation_prompt]
    generate[generate_initial_query]
    loop{attempt loop}
    validate[validate_query]
    valid{valid?}
    execute[execute_sparql_query]
    ok{execution ok?}
    correct[correct_query]
    trace[write_query_trace\nwrite_readable_query_trace]
    result[QueryPipelineResult]

    cli --> active --> pipeline
    pipeline --> package --> retrieve --> prompt --> generate --> loop
    loop --> validate --> valid
    valid -->|yes| execute --> ok
    valid -->|no| correct --> loop
    ok -->|yes| trace --> result
    ok -->|no| correct --> loop
    loop -->|max attempts reached| trace --> result
```

## Code Map

| Step | Function / Module |
|---|---|
| CLI query entrypoint | `query.py::main` |
| API query entrypoint | `run_query()` in `app/api/routes/query.py` |
| Active package lookup | `get_active_package()` in `app/domain/package.py` |
| Runtime orchestration | `run_query_pipeline()` in `app/domain/runtime/pipeline.py` |
| Attempt loop | `run_query_attempts()` in `pipeline.py` |
| Retrieve chunks | `retrieve_context()` in `app/domain/rag/retrieve_context.py` |
| Render initial prompt | `render_query_generation_prompt()` in `prompt_renderer.py` |
| Generate initial SPARQL | `generate_initial_query()` in `query_generation.py` |
| Validate SPARQL | `validate_query()` in `validation.py` |
| Execute SPARQL | `execute_sparql_query()` in `sparql_execution.py` |
| Correct failed query | `correct_query()` in `query_correction.py` |
| Write traces | `write_query_trace()`, `write_readable_query_trace()` in `query_trace.py` |

## Query Logs

```text
ontology_packages/<package>/logs/
  query.log
  query-latest.txt
  query-runs/<run-id>.txt
```

## Invariants

- `query.py` always uses the active package.
- `query.py` has no package argument and no endpoint override.
- Candidate SPARQL is executed only after validation passes.
- Validation or execution failures can trigger correction attempts.
- `--k` is retrieval top-k, not correction iterations.
