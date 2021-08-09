from kombu import binding
from kombu import Exchange
from kombu import Queue
from kombu.mixins import ConsumerProducerMixin
from worker import log
from loguru import logger


class BaseWorker(ConsumerProducerMixin):

    routing_key = None

    def __init__(self, connection, exchange):

        self.connection = connection

        self.exchange = exchange

        # Routing keys bindings
        bindings = [
            binding(exchange, routing_key="rq.all"),
            binding(exchange, routing_key="rq.#.{}".format(self.routing_key)),
        ]

        log.info(
            "Listening on {} with keys {}/{}",
            connection,
            "rq.all",
            "rq.#.{}".format(self.routing_key),
        )

        self.queues = [Queue("", exchange, bindings=bindings)]

        self.result_queue = Queue("results", Exchange("results"), routing_key="results")

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(queues=self.queues, on_message=self.on_request, accept=["json"]),
        ]

    def on_request(self, message):
        log.info(f"got request! {message}")
        with log.contextualize(corrid=message.properties["correlation_id"]):
            result = self.execute(message)

            log.info("Replying results to api..")
            self.producer.publish(
                {"result": result, "emitter": self.routing_key},
                exchange="",
                routing_key=self.result_queue.name,
                correlation_id=message.properties["correlation_id"],
                serializer="json",
                retry=True,
                declare=[self.result_queue],
            )
            log.info("Successfully replied!")
            message.ack()

    def execute(self, message):
        raise Exception("Please implement this method in your class")
