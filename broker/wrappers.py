import functools
from loguru import logger
from time import perf_counter


def catch(*, level="DEBUG", timing=False):
    """Loguru wrapper for contextualized tasks"""

    def wrapper(func):
        name = func.__name__  # noqa:F841

        @functools.wraps(func)
        def wrapped(task, *args, **kwargs):
            with logger.contextualize(task_id=task.request.id):
                try:
                    if timing:
                        start = perf_counter()
                    result = func(*args, **kwargs)
                    payload = {
                        "status": "success",
                        "journeys": result,
                    }
                except Exception as e:  # noqa
                    logger.error("Exception in task: {} ({})", e)
                    payload = {
                        "status": "error",
                        "error": str(e),
                    }
                finally:
                    if timing:
                        end = perf_counter() - start
                        logger.info("Task took {}s to execute", end)
            return payload

        return wrapped

    return wrapper
