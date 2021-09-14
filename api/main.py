from fastapi import FastAPI
from . import models

app = FastAPI()


@app.post("/journey")
def post_journey(journey: models.JourneyIn):
    if journey.is_valid():
        journey.submit()
        return {"journey_id": 123456}
    else:
        return {"status": "error"}


@app.get("/journey/{journey_uuid}")
def get_journey(uuid: str):
    return {"status": "success", "journey_uuid": uuid}


@app.get("/tools/healthz")
def healthcheck():
    return {"status": "ok", "message": "This is the healthcheck endpoint."}
