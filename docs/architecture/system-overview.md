# System Overview

This diagram shows the main runtime surfaces of the project and how ontology packages connect onboarding, activation, querying, and evaluation.

```mermaid
flowchart LR
    file[Ontology file\n.ttl .owl .rdf]
    endpoint[External SPARQL endpoint]
    onboard[onboard.py\nonboarding_workflow]
    package[Ontology package\nmetadata + context + chunks + settings]
    active[Active package pointer\nontology_packages/.active_package]
    fuseki[(Managed Fuseki dataset)]
    query[query.py / POST /query\nruntime pipeline]
    eval[evaluate.py\nevaluation runner]
    llm[LLM API]

    file --> onboard
    endpoint --> onboard
    onboard --> package
    onboard --> active
    onboard --> fuseki

    package --> active
    active --> query
    active --> eval
    active --> fuseki

    query --> llm
    query --> fuseki
    eval --> query
```

## Code Map

| Area | Main entrypoint | Main domain modules |
|---|---|---|
| Onboarding | `onboard.py` | `app/domain/ontology/onboarding_workflow.py` |
| Activation | `activate.py` | `app/domain/ontology/package_activation.py` |
| Querying | `query.py`, `app/api/routes/query.py` | `app/domain/runtime/pipeline.py` |
| Evaluation | `evaluate.py` | `evaluation/experiment_runner.py` |
| Fuseki integration | n/a | `app/clients/fuseki.py` |
| LLM integration | n/a | `app/clients/llm.py` |
| Package state | n/a | `app/domain/package.py` |

## Invariants

- `query.py` always queries the active package.
- `/query` always queries the active package.
- For local file packages, activation is what guarantees the managed Fuseki dataset matches the package.
- Evaluation names a package, but only to verify it is already active.
