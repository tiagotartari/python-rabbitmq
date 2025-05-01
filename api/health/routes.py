from fastapi import APIRouter
import logging

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])
@router.get("/health", status_code=200)
def status():
    return {"message": "ok"}