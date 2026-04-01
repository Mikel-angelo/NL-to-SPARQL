from fastapi import APIRouter


router = APIRouter(tags=["root"])


@router.get("/")
async def read_root() -> dict[str, str]:
    return {"message": "FastAPI project is running"}