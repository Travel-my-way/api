from fastapi import FastAPI
from .bv.settings import Settings
from api.bv.routers import journey, tools
from pathlib import Path

config_path = Path(__file__).with_name("logging_config.json")


def create_app() -> FastAPI:
    api = FastAPI()

    api.settings = Settings()
    api.include_router(journey.router)
    api.include_router(tools.router)

    return api


app = create_app()
