from pydantic import BaseSettings, Field, RedisDsn, validator
from typing import List


class Settings(BaseSettings):

    # Celery configuration
    broker_url: str = Field(..., env="CELERY_BROKER_URL")
    result_backend: RedisDsn = Field(..., env="CELERY_RESULT_BACKEND")
    task_default_exchange: str = "bonvoyage"
    task_default_exchange_type: str = "topic"
    task_default_routing_key: str = "journey.all"

    # Workers
    workers: List[str] = Field(..., env='WORKERS')

