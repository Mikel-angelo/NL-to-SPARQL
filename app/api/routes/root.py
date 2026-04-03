from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse

from app.core.config import settings


router = APIRouter(tags=["root"])
_APP_DIR = Path(__file__).resolve().parents[2]
_STATIC_INDEX = _APP_DIR / "static" / "index.html"
_CURRENT_DIR = Path(settings.storage_path) / "current"


@router.get("/", response_class=FileResponse)
async def read_root() -> FileResponse:
    return FileResponse(_STATIC_INDEX)


@router.get("/load-log", response_class=PlainTextResponse)
async def read_load_log() -> PlainTextResponse:
    path = _CURRENT_DIR / "load.log"
    if not path.exists():
        raise HTTPException(status_code=404, detail="load.log not found")
    return PlainTextResponse(path.read_text(encoding="utf-8"))


@router.get("/metadata", response_class=PlainTextResponse)
async def read_metadata() -> PlainTextResponse:
    path = _CURRENT_DIR / "metadata.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="metadata.json not found")
    return PlainTextResponse(path.read_text(encoding="utf-8"))
