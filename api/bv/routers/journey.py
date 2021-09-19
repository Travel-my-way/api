from fastapi import APIRouter, Request, Depends
from api.bv import models
from api.bv.celery import Client as Celery

router = APIRouter()


@router.post("/journey", tags=["journey"])
def post_journey(journey: models.Journey, celery: Celery = Depends()):
    r = celery.publish_journey(journey=journey)
    return {"journey_id": r.id}


@router.get("/journey/{uuid}", tags=["journey"])
def get_journey(request:Request, uuid: str, celery: Celery = Depends()):
    r = celery.get_result(uuid=uuid, logger = request.app.logger)
    request.app.logger.info(r)
    return {"status": "success", "journey_uuid": uuid}
