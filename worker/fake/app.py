from loguru import logger

from ..base import BaseWorker


class FakeWorker(BaseWorker):

    routing_key = "fake"

    def execute(self, message):

        logger.info("Got message: {}", message)
        return {"content": "ohai", "demo": 123456}
