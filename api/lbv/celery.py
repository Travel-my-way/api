from celery import Celery, signature, chord
from celery.result import AsyncResult

from flask import current_app


class Client(Celery):
    workers = []

    def __init__(self, config_prefix="CELERY", app=None):
        self.config_prefix = config_prefix
        super(Client, self).__init__("bonvoyage")

        if app is not None:
            self.init_app(app=app)

    def init_app(self, app):
        # Configuration
        self.config_from_object(app.config)

        # List of workers
        self.workers = app.config["WORKERS"]

        # Add to app registry
        if not hasattr(app, "extensions"):
            app.extensions = {}
        app.extensions[self.config_prefix.lower()] = self

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
