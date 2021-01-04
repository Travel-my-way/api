from ..base import BaseWorker
from loguru import logger


class TrainlineWorker(BaseWorker):

    routing_key = "train"

    def execute(self, message):

        logger.info("Got message: {}", message)
        return {
            "content": "ohai",
            "demo": 123456
        }
