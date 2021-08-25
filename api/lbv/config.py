import os
import sys

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    SECRET_KEY = os.environ.get("SECRET_KEY") or "SUPER-SECRET"
    LOGFILE = sys.stderr

    # Available workers
    WORKERS = ["blablacar", "planes", "kombo"]

    # Celery config
    broker_url = os.getenv("CELERY_BROKER_URL")
    result_backend = os.getenv("CELERY_RESULT_BACKEND")
    task_default_exchange = "bonvoyage"
    task_default_exchange_type = "topic"
    task_default_routing_key = "journey.all"


class DockerComposeConfig(Config):
    DEBUG = True
    LOG_BACKTRACE = True
    LOG_LEVEL = "DEBUG"
    REDIS_URL = "redis://redis:6379"


class DevelopmentConfig(Config):
    DEBUG = True
    LOG_BACKTRACE = True
    LOG_LEVEL = "DEBUG"


class ProductionConfig(Config):
    LOG_BACKTRACE = False
    LOG_LEVEL = "INFO"


config = {
    "dev": DevelopmentConfig,
    "docker-compose": DockerComposeConfig,
    "production": ProductionConfig,
    "default": DevelopmentConfig,
}
