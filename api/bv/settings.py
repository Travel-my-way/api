from pydantic import BaseSettings, Field, RedisDsn
from pathlib import Path


class Settings(BaseSettings):

    # Celery configuration
    broker_url: str = Field(..., env="CELERY_BROKER_URL")
    result_backend: RedisDsn = Field(..., env="CELERY_RESULT_BACKEND")
    task_default_exchange: str = "bonvoyage"
    task_default_exchange_type: str = "topic"
    task_default_routing_key: str = "journey.all"

    # Workers
    workers: str = Field(..., env='WORKERS')

    class Config:
        env_file = Path("..") / ".env"
