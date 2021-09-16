from datetime import date

from celery import Celery, signature, chord
from celery.result import AsyncResult

from api.bv.models import Journey
from api.bv.settings import Settings


class Client(Celery):
    workers = []

    def __init__(self, settings: Settings, config_prefix="CELERY"):
        self.config_prefix = config_prefix
        super(Client, self).__init__("bonvoyage")

        self.init_app(settings=settings)

    def init_app(self, settings: Settings):
        self.config_from_object(settings)

        # List of workers
        self.workers = settings.workers

    def publish_journey(self, journey: Journey):
        print(f"From: {journey.origin}, TO: {journey.destination}")
        return self.send_tasks(
            from_loc=journey.origin,
            to_loc=journey.destination,
            start_date=journey.start.strftime("%Y-%m-%d")
        )

    def send_tasks(
        self, from_loc: str, to_loc: str, start_date: str, workers: list = None
    ) -> AsyncResult:
        if workers is None:
            workers = self.workers
        print(f"Emitting to workers {workers}")
        kwargs = {"from_loc": from_loc, "to_loc": to_loc, "start_date": start_date}
        sigs = [
            signature(
                "worker",
                kwargs=kwargs,
                routing_key=f"journey.{k}",
                exchange="bonvoyage",
            )
            for k in workers
        ]
        broker = signature(
            "broker", kwargs=kwargs, routing_key="journey.broker", exchange="bonvoyage"
        )
        r = chord(sigs)(broker)

        return r


celery_app = Client(settings=Settings())
