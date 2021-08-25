import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from .celery import make_app

# Load .env from parent directory
env_path = Path("..") / ".env"
load_dotenv(dotenv_path=env_path)

# Custom logger with context data
log_format = (
    "<g>{time:YYYY-MM-DD HH:mm:ss.SSS}</g> <lr>|</lr> "
    "<lvl>{level: <8}</lvl> <lr>|</lr> "
    "<y>{extra[corrid]}</y> <lr>|</lr> "
    "<c>{name}</c>:<c>{function}</c>:<c>{line}</c> - <lvl>{message}</lvl>"
)
logger.remove()  # Remove all known handlers
logger.add(sys.stdout, format=log_format)
log = logger.bind(corrid="n/a")

app = make_app(name="broker")
