from ..celery import make_app
from celery import Task
from loguru import logger

app = make_app(name="planes")


class BaseTask(Task):
    def __init__(self):
        from .tasks import load_plane_db, load_airport_db  # Avoir circular imports

        self.airport_database = load_airport_db()
        self.plane_database = load_plane_db()

        logger.info("len airport_db: {}", len(self.airport_database))
        logger.info("len plane_db: {}", len(self.plane_database))
