from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
import sys

# Load .env from parent directory
load_dotenv(dotenv_path=Path("..") / ".env")

# Configuredefault logging forma
log_format = (
    "<g>{time:YYYY-MM-DD HH:mm:ss.SSS}</g> <lr>|</lr> "
    "<lvl>{level: <8}</lvl> <lr>|</lr> "
    "<y>{extra[corrid]}</y> <lr>|</lr> "
    "<y>{extra[task_id]}</y> <lr>|</lr> "
    "<c>{name}</c>:<c>{function}</c>:<c>{line}</c> - <lvl>{message}</lvl>"
)

logger.remove()  # Remove all known handlers
logger.configure(
    handlers=[dict(sink=sys.stdout, format=log_format)],
    extra={"corrid": "-", "task_id": "-"},
)
