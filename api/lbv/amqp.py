import uuid

from kombu import Connection
from kombu import Exchange
from kombu import Queue

# Heaviliy borrowed from https://github.com/flask-rabmq/flask-rabmq/blob/master/flask_rabmq/__init__.py


class RabbitMQ:

    config = None
    connection = None
    exchange = None

    def init_app(self, app):
        self.config = app.config
        self.connection = Connection(self.config.get("RABMQ_RABBITMQ_URL"))
        self.exchange = Exchange(
            name=self.config.get("RABMQ_SEND_EXCHANGE_NAME"),
            type=self.config.get("RABMQ_SEND_EXCHANGE_TYPE") or "topic",
            auto_delete=False,
            durable=True,
        )

    def send(self, body, routing_key):
        correlation_id = str(uuid.uuid4())

        # Dedicated reply-to Q, expiring after 30 seconds
        reply_to = Queue(
            name=correlation_id, expires=self.config.get("RABMQ_REPLY_EXPIRES")
        )

        producer = self.connection.Producer(serializer="json")

        producer.publish(
            body,
            exchange=self.exchange,
            routing_key=routing_key,
            reply_to=reply_to.name,
            correlation_id=correlation_id,
        )

        return correlation_id


ramq = RabbitMQ()
