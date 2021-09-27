from flask import current_app, jsonify
from flask_restful import reqparse
from flask_restful import Resource


# Args parser
parser = reqparse.RequestParser()
parser.add_argument("from")
parser.add_argument("to")
parser.add_argument("start")
parser.add_argument("nb_passenger")


class Journey(Resource):
    def post(self):
        current_app.logger.info("POSTing request")
        args = parser.parse_args()

        # Emit the request to workers then broker
        r = current_app.extensions["celery"].send_tasks(
            from_loc=args["from"], to_loc=args["to"], start_date=args["start"], nb_passenger=args["nb_passenger"]
        )
        return {"uuid": r.id}, 201
