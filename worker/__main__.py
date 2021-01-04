import importlib
import os

import click
from kombu import Connection
from kombu import Exchange

from . import log


@click.command()
@click.option("--name", prompt="Worker name", help="Worker to launch")
def run(name):
    log.info("Initializing {} worker", name)

    # Import Module
    module = importlib.import_module(f"worker.{name}")
    worker_class = getattr(module, "Worker")

    with Connection(os.getenv("RABMQ_RABBITMQ_URL")) as conn:
        exchange = Exchange(
            name=os.getenv("RABMQ_SEND_EXCHANGE_NAME"),
            type=os.getenv("RABMQ_SEND_EXCHANGE_TYPE"),
            durable=False,
        )
        worker = worker_class(connection=conn, exchange=exchange)
        log.info(f"Launching {name} worker...")
        worker.run()


if __name__ == "__main__":
    run()
