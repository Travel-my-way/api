from fastapi import Request, Depends
from celery import Celery, signature, chord, states
from celery.result import AsyncResult

from api.bv.models import Journey
from api.bv.settings import Settings


class Client(Celery):
    workers = []

    def __init__(self, config_prefix="CELERY"):
        self.config_prefix = config_prefix
        super(Client, self).__init__("bonvoyage")

        self.init_app(settings=Settings())

    def init_app(self, settings: Settings):
        self.config_from_object(settings)

        # List of workers
        self.workers = settings.workers

    def publish_journey(self, journey: Journey) -> AsyncResult:
        print(f"Emitting: {journey} to {self.workers} workers")

        # Preparing worker's signatures
        kwargs = journey.as_celery_kwargs()
        workers_sigs = [
            signature(
                "worker",
                kwargs=kwargs,
                routing_key=f"journey.{k}",
                exchange="bonvoyage",
            )
            for k in self.workers
        ]
        # Broker sig, then celery chord for orchestration
        broker_signature = signature(
            "broker", kwargs=kwargs, routing_key="journey.broker", exchange="bonvoyage"
        )
        r = chord(workers_sigs)(broker_signature)
        return r

    def get_result(self, uuid: str, logger):
        logger.info("Fetching results for {}", uuid)
        try:
            task_result = AsyncResult(id=uuid)
            if task_result.successful():
                # Everything is OK, return the result
                res = (task_result.result, 200)
            elif task_result.status == states.FAILURE:
                res = ({"error": task_result.result}, 500)
            else:
                # Task still running,please come back later
                res = ({}, 204)
        except TimeoutError:
            # Task (with all workers) took too much time.
            res = (
                {"error": "Journey took too much time to calculate, it was timeouted"},
                410,
            )
        finally:
            return res
