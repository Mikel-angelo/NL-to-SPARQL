"""Small utility routes for the static UI and active package artifacts."""

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse

from app.core.config import settings
from app.domain.package import PackageNotFoundError, get_active_package


router = APIRouter(tags=["root"])
_APP_DIR = Path(__file__).resolve().parents[2]
_STATIC_INDEX = _APP_DIR / "static" / "index.html"


def _active_package_dir() -> Path:
    try:
        return get_active_package(settings.ontology_packages_path)
    except PackageNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/", response_class=FileResponse)
async def read_root() -> FileResponse:
    """Serve the static HTML page used for manual interaction."""
    return FileResponse(_STATIC_INDEX)


@router.get("/load-log", response_class=PlainTextResponse)
async def read_load_log() -> PlainTextResponse:
    path = _active_package_dir() / "logs" / "onboard.log"
    if not path.exists():
        raise HTTPException(status_code=404, detail="load.log not found")
    return PlainTextResponse(path.read_text(encoding="utf-8"))


@router.get("/metadata", response_class=PlainTextResponse)
async def read_metadata() -> PlainTextResponse:
    path = _active_package_dir() / "metadata.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="metadata.json not found")
    return PlainTextResponse(path.read_text(encoding="utf-8"))


@router.get("/query-pipeline-log", response_class=PlainTextResponse)
async def read_query_pipeline_log() -> PlainTextResponse:
    path = _active_package_dir() / "logs" / "query.log"
    if not path.exists():
        raise HTTPException(status_code=404, detail="query_pipeline_log.json not found")
    return PlainTextResponse(path.read_text(encoding="utf-8"))
