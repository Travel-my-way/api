import json

from flask import current_app
from flask_restful import reqparse
from flask_restful import Resource

from api.lbv.amqp import ramq
from api.lbv.redis import redis_client


# Args parser
parser = reqparse.RequestParser()
parser.add_argument("from")
parser.add_argument("to")
parser.add_argument("start")


class Journey(Resource):
    def post(self):
        current_app.logger.info("POSTing request")
        args = parser.parse_args()

        params = {"from": args["from"], "to": args["to"], "start": args["start"]}

        req_id = ramq.send(body=params, routing_key="rq.all")

        # Store details in redis.
        redis_client.set("request_id:{} type:params".format(req_id), json.dumps(params))

        return {"request_id": req_id}, 201
