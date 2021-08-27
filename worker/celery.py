from celery import Celery
from loguru import logger
from worker.exception import WorkerException
import os
import importlib


def make_app(name, routing_key=None, init_fn=None, task_cls=None):
    clry = Celery("bonvoyage", task_cls=task_cls)
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
    if init_fn is not None:
        logger.info("Worker init using {}()", init_fn)
        calls = init_fn.split(":")
        if len(calls) != 2:
            raise WorkerException(
                "Init function must be provided as module:name string"
            )
        try:
            mod = importlib.import_module(calls[0])
            func = getattr(mod, calls[1])
            g = func()
            return clry, g
        except AttributeError:  # noqa
            raise WorkerException("No function {} found in {} module", calls[1], mod)

    clry.autodiscover_tasks(packages=[f"worker.{name}"])
    return clry
