from ..celery import make_app
from celery import Task
from loguru import logger

app = make_app(name="kombo")


class BaseTask(Task):
    def __init__(self):
        logger.info("Starting city update")
        from .tasks import update_city_list  # Prevent circular imports

        self.city_db = update_city_list()

        logger.info("Update ended")
