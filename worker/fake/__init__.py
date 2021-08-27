from worker.celery import make_app
import os

app = make_app(name="fake", routing_key=os.getenv("FAKE_NAME", "fake"))
