from ..celery import make_app
from celery import Task

app = make_app(name="ors")


class BaseTask(Task):
    def __init__(self):
        pass
