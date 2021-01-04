from . import log
import click
from .client import Client
from kombu import Connection
import os

@click.command()
def run():
    log.info("Running broker")

    with Connection(os.getenv("RABMQ_RABBITMQ_URL")) as conn:
        broker = Client(
            connection=conn
        )
        broker.run()


if __name__ == "__main__":
    run()
