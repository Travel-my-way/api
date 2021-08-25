from ..celery import make_app
from celery import Task
from loguru import logger

app = make_app(name="planes")


class BaseTask(Task):
    def __init__(self):
        pass
