from fastapi import APIRouter, Request
from api.bv import models

router = APIRouter()


@router.post("/journey", tags=["journey"])
def post_journey(request: Request, journey: models.JourneyIn):
    if journey.is_valid():
        request.app.logger.info("valid journey, sending tasks")
        uuid = journey.submit()
        return {"journey_id": uuid}
    else:
        return {"status": "error"}


@router.get("/journey/{uuid}", tags=["journey"])
def get_journey(uuid: str):
    return {"status": "success", "journey_uuid": uuid}
