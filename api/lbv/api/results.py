import celery.states
from flask import current_app
from flask_restful import reqparse
from flask_restful import Resource
from loguru import logger

from celery.result import AsyncResult

from .exception import ApiException

# RestFul parser
parser = reqparse.RequestParser()
parser.add_argument("request_id", type=str, required=True)


# API endpoint
class Results(Resource):
    def get(self):
        current_app.logger.info("GETting results")
        args = parser.parse_args()

        try:
            task_result = AsyncResult(id=args["request_id"])
            if task_result.successful():
                # Everything is OK, return the result
                return task_result.result, 200
            elif task_result.status == celery.states.FAILURE:
                return {"error": task_result.result}, 500
            else:
                # Task still running,please come back later
                return {}, 204
        except TimeoutError:
            # Task (with all workers) took too much time.
            return (
                {"error": "Journey took too much time to calculate, it was timeouted"},
                410,
            )
