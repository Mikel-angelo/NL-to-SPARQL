"""Ontology onboarding package APIs."""

from app.domain.ontology.onboarding_extraction import ExtractionResult, extract_metadata
from app.domain.ontology.onboard_pipeline import OnboardingResult, onboard_ontology_file, onboard_sparql_endpoint

__all__ = [
    "ExtractionResult",
    "OnboardingResult",
    "extract_metadata",
    "onboard_ontology_file",
    "onboard_sparql_endpoint",
]
