from flask import current_app
from flask_restful import Resource, reqparse
from loguru import logger
from . import models
from .exception import ApiException

# RestFul parser
parser = reqparse.RequestParser()
parser.add_argument('request_id', type=str, required=True)
parser.add_argument('partial', type=bool, default=False)


# API endpoint
class Results(Resource):

    def get(self):
        current_app.logger.info("GETting results")
        args = parser.parse_args()

        if args['partial'] is True:
            logger.info("Returning partial results")
            results = models.PartialResults(request_id=args['request_id'])
        else:
            logger.info("Returning final results")
            results = models.FinalResults(request_id=args['request_id'])

        try:
            content = results.fetch()
            return content, 202
        except ApiException as ae:
            return {'error': ae.message}, 410
