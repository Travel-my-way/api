from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/tools/healthz")
def healthcheck():
    return {"status": "ok", "message": "This is the healthcheck endpoint."}