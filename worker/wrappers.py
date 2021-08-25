import functools
from loguru import logger
from time import perf_counter


def catch(*, level="DEBUG", timing=False):
    """Loguru wrapper for contextualized tasks"""

    def wrapper(func):
        name = func.__name__  # noqa: F841

        @functools.wraps(func)
        def wrapped(task, *args, **kwargs):
            try:
                corr_id = task.request.chord["options"]["task_id"]
            except Exception:  # noqa
                corr_id = "N/A"

            # Get worker name
            worker_name = task.request.delivery_info["routing_key"].replace(
                "journey.", ""
            )

            with logger.contextualize(corrid=corr_id, task_id=task.request.id):
                try:
                    if timing:
                        start = perf_counter()
                    result = func(self=task, *args, **kwargs)
                    payload = {
                        "status": "success",
                        "worker": worker_name,
                        "result": result,
                    }
                except Exception as e:  # noqa
                    logger.error("Exception in task: {}", e)
                    payload = {
                        "status": "error",
                        "worker": worker_name,
                        "error": str(e),
                    }
                finally:
                    if timing:
                        end = perf_counter() - start
                        logger.info("Task took {}s to execute", end)
            return payload

        return wrapped

    return wrapper
