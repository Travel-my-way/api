from fastapi import APIRouter, Request, Depends, Query
from typing import Union
from api.bv import models
from api.bv.celery import Client as Celery
from loguru import logger

router = APIRouter()

@router.post("/journey", tags=["journey"])
def post_journey(
        origin=Query(default=None, alias="from"),
        destination: Union[str, None] = Query(default=None, alias="to"),
        start: Union[int, None] = Query(default=None),
        nb_passenger: Union[int, None] = Query(default=None),
        celery: Celery = Depends()):
    logger.info("Journey requested")
    journey = models.Journey(
        origin=origin,
        destination=destination,
        start=start,
        nb_passenger=nb_passenger
    )
    r = celery.publish_journey(journey=journey)
    return {"journey_id": r.id}


@router.get("/results", tags=["journey"])
def get_journey(request_id: str, celery: Celery = Depends()):
    r = celery.get_result(uuid=request_id)
    return r
