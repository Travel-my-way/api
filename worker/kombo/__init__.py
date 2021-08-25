from ..celery import make_app
from celery import Task
from loguru import logger


def global_init():
    logger.info("Loading cities DB")
    from .tasks import update_city_list

    from worker.carbon.emission import init_carbon

    # Initglobal carbon frame
    init_carbon()

    return update_city_list()


app = make_app(name="kombo")

# Init global values shared between runners
city_db = global_init()


class BaseTask(Task):
    def __init__(self):
        pass
