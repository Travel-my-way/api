from ..celery import make_app
from celery import Task
import os
from loguru import logger

app = make_app(name="ferries", routing_key=os.getenv("FAKE_NAME", "fake"))


class BaseTask(Task):
    def __init__(self):
        from .tasks import (
            load_ferry_db,
            load_route_db,
            update_city_list,
        )  # Import here to prevent circular imports

        self.ferry_database = load_ferry_db()
        self.route_database = load_route_db()
        self.city_db = update_city_list()

        logger.info("len ferry_db: {}", len(self.ferry_database))
        logger.info("len route_db: {}", len(self.route_database))
