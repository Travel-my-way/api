from ..celery import make_app
from celery import Task
import os

app = make_app(name="fake", routing_key=os.getenv("FAKE_NAME", "fake"))


class BaseTask(Task):
    def __init__(self):
        # Run global init here
        pass
