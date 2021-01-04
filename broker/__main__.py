import os

import click
from kombu import Connection

from . import log
from .client import Client


@click.command()
def run():
    log.info("Running broker")

    with Connection(os.getenv("RABMQ_RABBITMQ_URL")) as conn:
        broker = Client(connection=conn)
        broker.run()


if __name__ == "__main__":
    run()
