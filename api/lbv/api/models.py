import json
from typing import Any
from typing import NoReturn

from loguru import logger

from .exception import ApiException
from api.lbv.redis import redis_client


class BaseResults:

    result_type = None
    results = None

    def __init__(self, request_id: str) -> None:
        self.request_id = request_id

    @property
    def key_name(self) -> str:
        return "request_id:{} type:{}".format(self.request_id, self.result_type)

    def get_results(self) -> NoReturn:
        raise ApiException("Please implement method in you subclass")

    def get_request_params(self) -> dict:
        """Returns the query parameters provided by the user for this request_id"""
        key = redis_client.get("request_id:{} type:params".format(self.request_id))
        return json.loads(key)

    def fetch(self) -> Any:
        if redis_client.exists(self.key_name):
            logger.info("Found matching key in redis ({})", self.key_name)
            results = self.get_results()
            params = self.get_request_params()

            return {
                "params": params,
                "partial": True if self.result_type == "partial_results" else False,
                "results": results,
            }
        else:
            logger.error("No results found in redis for key {}!", self.key_name)
            raise ApiException(
                message="No results found for this query", request_id=self.request_id
            )


class PartialResults(BaseResults):
    """Final results for a given query."""

    result_type = "partial_results"

    def get_results(self) -> dict:
        res = redis_client.smembers(self.key_name)
        objs = [json.loads(_) for _ in res]
        content = {}

        for o in objs:
            content[o["emitter"]] = o["result"]

        return content


class FinalResults(BaseResults):
    """Final results for a given query."""

    result_type = "final_results"

    def get_results(self) -> dict:
        key = redis_client.get(self.key_name)

        return json.loads(key)
