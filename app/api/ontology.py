from fastapi import APIRouter, File, UploadFile

from app.services.ontology import OntologyService


router = APIRouter(prefix="/ontology", tags=["ontology"])

ontology_service = OntologyService()


@router.post("/upload")
async def upload_ontology(file: UploadFile = File(...)) -> dict[str, str]:
    ontology_id, dataset_name = await ontology_service.upload_ontology(file)
    return {"ontology_id": ontology_id, "dataset_name": dataset_name}
