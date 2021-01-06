import os

import click
from kombu import Connection

from . import log
from .client import Client


@click.command()
def run():
    log.info("Starting broker")
    mq_url = os.getenv("RABMQ_RABBITMQ_URL")
    redis_url = os.getenv("REDIS_URL")

    with Connection(mq_url) as conn:
        log.info("Connected to AMQP: {}", mq_url)
        broker = Client(connection=conn, redis_url=redis_url)
        log.info("Broker started, waiting for messages...")
        broker.run()


if __name__ == "__main__":
    run()
