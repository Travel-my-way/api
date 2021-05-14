import os
from . import create_app

app = create_app(os.getenv("TARGET_ENV", "production"))
