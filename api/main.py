from fastapi import FastAPI
from .bv import logging as bv_logging
from .bv.settings import Settings
from api.bv.routers import journey, tools
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

config_path = Path(__file__).with_name("logging_config.json")


def create_app() -> FastAPI:
    api = FastAPI()

    api.settings = Settings()
    api.include_router(journey.router)
    api.include_router(tools.router)
    log = bv_logging.CustomizeLogger.make_logger(config_path)
    api.logger = log

    return api


app = create_app()
