from flask import Flask
import logging

from .api import blueprint as api_blueprint
from .tools import tools_bp
from .config import config
from .log import InterceptHandler
from .celery import Client as CeleryClient


def create_app(env):
    app = Flask(__name__, instance_relative_config=True)

    # Load default config
    app.config.from_object(config[env])

    # Use gunicorn logging if in production.
    if env == "production":
        gunicorn_logger = logging.getLogger("gunicorn.error")
        app.logger.handlers = gunicorn_logger.handlers
        app.logger.setLevel(gunicorn_logger.level)

    app.logger.info(f"Running in {env} mode")

    initialize_extensions(app=app)
    # Init Celery
    celery = CeleryClient()
    celery.init_app(app=app)

    # API Blueprint
    app.register_blueprint(api_blueprint)

    # Technical routes
    app.register_blueprint(tools_bp, url_prefix="/tools")

    app.logger.info("Clients & blueprints loaded and started, let's roll!")
    return app


def initialize_extensions(app):
    pass
