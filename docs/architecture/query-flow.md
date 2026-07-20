# Query Flow

Querying starts from the active package, retrieves ontology context, asks the LLM for SPARQL, validates it, executes it, and optionally asks for corrections.

```mermaid
flowchart TD
    cli[query.py\nor POST /query]
    active[get_active_package]
    pipeline[run_query_pipeline]
    package[read package artifacts\nmetadata.json\nsettings.json\nontology_context.json]
    config[resolve runtime settings\nmodel\nquery_endpoint\nschema top-k\nchunking strategy\nABox flags/top-k\ncorrection attempts]
    retrieve[retrieve_context\nuses selected indexes/<strategy>\nand k retrieved chunks]
    retrieve_abox{ABox RAG enabled?}
    abox[retrieve_abox_context\nuses indexes/abox\nand ABox top-k]
    prompt[render_query_generation_prompt\nquery_generation_prompt.j2]
    prompt_fields[prompt fields\nquestion\nretrieved schema chunks\noptional ABox candidates\nprefix declarations\nontology/dataset labels\noutput + label rules]
    generate[generate_initial_query\nLLM API]
    loop{attempt loop}
    normalize[normalize query prefixes\nadd declared PREFIX block]
    validate[validate_query]
    syntactic[syntactic stage\nSPARQL parser]
    prefix[prefix stage\nunknown prefix check]
    vocab[vocabulary stage\nknown classes/properties]
    structural[structural stage\nWHERE + variables + shape checks]
    valid{valid?}
    execute[execute_sparql_query]
    exec_stage[execution stage\nEXECUTION_OK / EXECUTION_ERROR]
    ok{execution ok?}
    correction_prompt[render_correction_prompt\nquery_correction_prompt.j2\nsame retrieved chunks\nfailed query + errors + prefixes]
    correct[correct_query\nLLM API]
    trace[write_query_trace\nwrite_readable_query_trace]
    result[QueryPipelineResult]

    cli --> active --> pipeline
    pipeline --> package --> config --> retrieve --> retrieve_abox
    retrieve_abox -->|yes| abox --> prompt
    retrieve_abox -->|no| prompt
    prompt --> prompt_fields --> generate --> loop
    loop --> normalize --> validate
    validate --> syntactic --> prefix --> vocab --> structural --> valid
    valid -->|yes| execute --> exec_stage --> ok
    valid -->|no| correction_prompt --> correct --> loop
    ok -->|yes| trace --> result
    ok -->|no| correction_prompt
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
| Runtime settings | CLI/API overrides, then `app/core/config.py`; package settings provide endpoint infrastructure |
| Retrieve chunks from selected index | `retrieve_context(..., k=effective_k, chunking=effective_chunking)` in `app/domain/rag/retrieve_context.py` |
| Retrieve ABox instance chunks | `retrieve_abox_context(..., k=effective_abox_k)` in `app/domain/rag/retrieve_abox_context.py` |
| Render initial prompt | `render_query_generation_prompt()` in `prompt_renderer.py` |
| Initial prompt template | `app/domain/runtime/templates/query_generation_prompt.j2` |
| Generate initial SPARQL | `generate_initial_query()` in `query_generation.py` |
| Normalize prefixes | `validate_query()` calls `_normalize_query()` in `validation.py` |
| Syntactic validation | `_syntactic_validation()` in `validation.py` |
| Prefix validation | `_prefix_validation()` in `validation.py` |
| Vocabulary validation | `_vocabulary_validation()` in `validation.py` |
| Structural validation | `_structural_validation()` in `validation.py` |
| Execute SPARQL | `execute_sparql_query()` in `sparql_execution.py` |
| Execution trace stage | `execution_stage_result()` in `sparql_execution.py` |
| Correct failed query | `correct_query()` in `query_correction.py` |
| Correction prompt template | `app/domain/runtime/templates/query_correction_prompt.j2` |
| Write traces | `write_query_trace()`, `write_readable_query_trace()` in `query_trace.py` |

## Prompt Inputs

The initial generation prompt contains:

- user question
- retrieved ontology chunks, controlled by retrieval top-k and selected chunking strategy
- optional retrieved ABox instance candidates, controlled by `app/core/config.py` defaults or caller overrides, plus ABox top-k
- ontology label and dataset label, explicitly marked as labels and not prefixes
- auto-generated prefix declarations from `ontology_context.json`
- prefix usage rules
- result-shape rules, including label preference for entity answers
- output format rules

The correction prompt contains:

- original question
- same retrieved chunks used in the initial generation prompt
- same retrieved ABox candidates, if ABox RAG was enabled
- failed query
- validation or execution errors
- available prefix declarations
- prefix and result-shape rules

## Validation And Execution Stages

```text
formal validation:
  1. syntactic   -> SPARQL parser
  2. prefix      -> rejects undeclared prefixes
  3. vocabulary  -> checks referenced ontology classes/properties
  4. structural  -> WHERE, projected variables, broad query shape

endpoint execution:
  5. execution   -> EXECUTION_OK or EXECUTION_ERROR
```

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
- `query.py --chunking` selects one prebuilt package index; it does not rebuild indexes.
- `query.py --abox-rag` enables instance retrieval from `indexes/abox`.
- `query.py --no-abox-rag` disables instance retrieval for one query.
- `query.py --abox-k` controls ABox retrieval top-k.
- `query.py --reactive-abox-discovery` enables the legacy endpoint-based ABox discovery step during empty-result correction.
- `query.py --no-reactive-abox-discovery` disables reactive ABox discovery for one query.
- If model, schema top-k, ABox use, ABox top-k, reactive ABox discovery, chunking, or corrections are omitted, `app/core/config.py` supplies the defaults.
- Candidate SPARQL is executed only after validation passes.
- Validation or execution failures can trigger correction attempts.
- `--k` is retrieval top-k, not correction iterations.
- `--abox-k` is instance retrieval top-k, not correction iterations.
- `--corrections` is the maximum number of correction loop attempts for that query.
