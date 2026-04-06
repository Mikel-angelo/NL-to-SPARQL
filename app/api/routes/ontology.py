from fastapi import APIRouter, File, UploadFile

from app.services.onboarding.ontology_onboarding import OntologyOnboardingService


router = APIRouter(prefix="/ontology", tags=["ontology"])

ontology_onboarding_service = OntologyOnboardingService()


@router.post("/load")
async def load_ontology(file: UploadFile = File(...)) -> dict[str, str]:
    """Load one ontology file, replace the current Fuseki dataset, and refresh local current files."""
    return await ontology_onboarding_service.load_ontology(file)
