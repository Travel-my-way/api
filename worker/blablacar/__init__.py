from ..celery import make_app
from celery import Task

app = make_app(name="blablacar")


class BaseTask(Task):
    def __init__(self):
        # Run global init here
        pass
