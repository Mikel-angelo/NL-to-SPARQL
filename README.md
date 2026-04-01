# NL-to-SPARQL

FastAPI scaffold with a router-based app layout.

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

## Structure

```text
app/
  api/
    routes/
  core/
  services/
  main.py
```

Docs will be available at `http://127.0.0.1:8000/docs`.

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

Fuseki UI will be available at `http://127.0.0.1:3030`.
Datasets are not pre-created in Compose and can be created later as needed.
Fuseki data is stored at the project root in `fuseki-data`.
Admin login: `admin`
Admin password: `admin`