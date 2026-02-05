from fastapi import APIRouter, Depends
from ..auth import require_api_key
from ..models import HealthOut

router = APIRouter()

@router.get("/health", response_model=HealthOut)
def health(_: None = Depends(require_api_key)):
    return HealthOut(status="ok")
