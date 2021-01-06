import os
import sys

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    SECRET_KEY = os.environ.get("SECRET_KEY") or "SUPER-SECRET"
    LOGFILE = sys.stderr

    # Redis
    REDIS_URL = os.getenv("REDIS_URL")

    # Default rabbitMQ setup
    RABMQ_RABBITMQ_URL = os.getenv(
        "RABMQ_RABBITMQ_URL", default="amqp://user:bitnami@localhost:5672/"
    )
    RABMQ_SEND_EXCHANGE_NAME = os.getenv("RABMQ_SEND_EXCHANGE_NAME", default="tmw")
    RABMQ_SEND_EXCHANGE_TYPE = os.getenv("RABMQ_SEND_EXCHANGE_TYPE", default="topic")

    RABMQ_REPLY_EXPIRES = os.getenv("RABMQ_REPLY_EXPIRES", default=30)


class DockerComposeConfig(Config):
    DEBUG = True
    LOG_BACKTRACE = True
    LOG_LEVEL = "DEBUG"
    RABMQ_RABBITMQ_URL = "amqp://user:bitnami@rabbitmq:5672/"
    REDIS_URL = "redis://redis:6379"


class DevelopmentConfig(Config):
    DEBUG = True
    LOG_BACKTRACE = True
    LOG_LEVEL = "DEBUG"

    RABMQ_REPLY_EXPIRES = 180


class ProductionConfig(Config):
    LOG_BACKTRACE = False
    LOG_LEVEL = "INFO"


config = {
    "dev": DevelopmentConfig,
    "docker-compose": DockerComposeConfig,
    "production": ProductionConfig,
    "default": DevelopmentConfig,
}
