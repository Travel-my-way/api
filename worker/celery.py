from celery import Celery
import os


def make_app(name, routing_key=None):
    clry = Celery("bonvoyage", task_cls=f"worker.{name}:BaseTask")
    clry.conf.task_default_exchange = os.getenv(
        "CELERY_EXCHANGE_NAME", default="bonvoyage"
    )

    # Set routing key to name if none provided
    r_key = routing_key if routing_key is not None else name

    clry.worker = name

    clry.conf.task_default_exchange_type = "topic"
    clry.conf.task_default_queue = r_key
    clry.conf.task_default_routing_key = f"journey.{r_key}"
    clry.autodiscover_tasks(packages=[f"worker.{name}"])

    return clry
