from flask import Blueprint
from .journey import Journey
from .results import Results
from flask_restful import Api

blueprint = Blueprint('api', __name__, template_folder='templates')

# Init Flask-Restful
api = Api(blueprint)

api.add_resource(Journey, "/journey")
api.add_resource(Results, "/results")
