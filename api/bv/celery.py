from celery import Celery, signature, chord
from celery.result import AsyncResult
from .settings import Settings

class Client(Celery):
    workers = []

    def __init__(self, settings, config_prefix="CELERY"):
        self.config_prefix = config_prefix
        super(Client, self).__init__("bonvoyage")

        self.init_app(settings)

    def init_app(self, settings: Settings):
        # Configuration
        self.config_from_object(settings)

        # List of workers
        self.workers = settings.workers

        app.logger.info(f"Celery configured with workers {self.workers}")

    def send_tasks(
        self, from_loc: str, to_loc: str, start_date: str, workers: list = None
    ) -> AsyncResult:
        if workers is None:
            workers = self.workers
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
