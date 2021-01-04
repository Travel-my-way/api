from typing import NoReturn

from kombu.mixins import ConsumerMixin
from kombu import Queue, Exchange
from loguru import logger
from redis import Redis
import os
import json


class Client(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection

        self.redis = Redis.from_url(os.getenv("REDIS_URL"))

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(
                [
                    Queue('results', Exchange('results'), routing_key='results')
                ],
                callbacks=[self.on_message],
                accept=['json']
            ),
        ]

    def on_message(self, body, message):
        correlation_id = message.properties['correlation_id']
        with logger.contextualize(corrid=correlation_id):
            set_name = "request_id:{} type:partial_results".format(correlation_id)

            logger.info("Adding message to collation set...")
            try:
                self.redis.sadd(
                    set_name,
                    json.dumps(body)
                )
            except Exception as e:
                logger.error("An error occured during Redis insert: {}", e)
            finally:
                message.ack()  # Always ack
                results = self.compute_results(request_id=correlation_id)
                self.store_results(request_id=correlation_id, results=results)

    def store_results(self, request_id: str, results: dict) -> None:
        self.redis.set(
            "request_id:{} type:final_results".format(request_id),
            json.dumps(results)
        )

    def compute_results(self, request_id: str) -> dict:
        logger.info("Computing result for set {}", request_id)

        # Load all partial results
        set = self.redis.smembers("request_id:{} type:partial_results".format(request_id))

        # MAGIC happens here !
        ## The following is NOT magic :)
        partials = [json.loads(_) for _ in set]

        content = {}

        for o in partials:
            content[o['emitter']] = o['result']

        return content
