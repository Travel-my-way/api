from fastapi import FastAPI
from .bv import logging as bv_logging
from .bv.settings import Settings
from api.bv.routers import journey
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

config_path=Path(__file__).with_name("logging_config.json")


def create_app() -> FastAPI:
    app = FastAPI()

    app.settings = Settings()
    app.include_router(journey.router)
    logger = bv_logging.CustomizeLogger.make_logger(config_path)
    app.logger = logger

    return app


app = create_app()


@app.get("/tools/healthz")
def healthcheck():
    return {"status": "ok", "message": "This is the healthcheck endpoint."}
