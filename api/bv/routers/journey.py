from fastapi import APIRouter
from api.bv import models
from api.bv.celery import celery_app

router = APIRouter()


@router.post("/journey", tags=["journey"])
def post_journey(journey: models.Journey):
    r = celery_app.publish_journey(journey=journey)
    return {"journey_id": r.id}


@router.get("/journey/{uuid}", tags=["journey"])
def get_journey(uuid: str):
    return {"status": "success", "journey_uuid": uuid}
