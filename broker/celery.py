from celery import Celery
import os


def make_app(name: str) -> Celery:
    clry = Celery("bonvoyage")
    clry.conf.task_default_exchange = os.getenv(
        "CELERY_EXCHANGE_NAME", default="bonvoyage"
    )
    clry.conf.task_default_exchange_type = "topic"
    clry.conf.task_default_queue = name
    clry.conf.task_default_routing_key = f"journey.{name}"
    clry.autodiscover_tasks(packages=["broker"])

    return clry
