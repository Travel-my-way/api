from . import app
from worker import wrappers, utils
from loguru import logger
from .. import constants
from .. import config as tmw_api_keys
from .. import TMW

from worker.carbon import emission


import openrouteservice
from datetime import timedelta, datetime as dt

"""
OPEN ROUTE SERVICES FUNCTIONS
"""


def start_ors_client():
    ors_api_key = tmw_api_keys.ORS_API_KEY
    ors_client = openrouteservice.Client(
        key=ors_api_key
    )  # Specify your personal API key
    return ors_client


def ors_profile(profile):  # Should be integrated into CONSTANTS.py
    dict_ors_profile = {
        "driving-car": constants.TYPE_CAR,
        "driving-hgv": "",
        "foot-walking": "walk",
        "foot-hiking": "walk",
        "cycling-regular": "bike",
        "cycling-road": "bike",
        "cycling-mountain": "bike",
        "cycling-electric": "bike",
    }
    return dict_ors_profile[profile]


def ors_query_directions(
    query,
    profile="driving-car",
    toll_price=True,
    _id=0,
    geometry=False,
    avoid_ferries=True,
):
    """
    start (class point)
    end (class point)
    profile= ["driving-car", "driving-hgv", "foot-walking","foot-hiking", "cycling-regular", "cycling-road",
    "cycling-mountain", "cycling-electric",]
    """
    ors_client = start_ors_client()
    coord = [
        query["start_point"][::-1],
        query["end_point"][::-1],
    ]  # WARNING it seems that [lon,lat] are not in the same order than for other API.
    if avoid_ferries:
        options = {"avoid_features": ["ferries"]}
    else:
        options = {}
    try:
        ors_step = ors_client.directions(
            coord,
            profile=profile,
            instructions=False,
            geometry=geometry,
            options=options,
        )
    except Exception as e:
        logger.error(e)
        return None

    # geojson = convert.decode_polyline(ors_step['routes'][0]['geometry'])
    # logger.info(ors_step)

    local_distance = ors_step["routes"][0]["summary"]["distance"]
    local_emissions = emission.calculate_co2_emissions(
        constants.TYPE_CAR, local_distance
    )

    formated_date = dt.strptime(query["departure_date"], "%Y-%m-%d")

    step = TMW.Journey_step(
        _id,
        _type=ors_profile(profile),
        label=profile,
        distance_m=local_distance,
        duration_s=ors_step["routes"][0]["summary"]["duration"],
        price_EUR=[ors_gas_price(ors_step["routes"][0]["summary"]["distance"])],
        gCO2=local_emissions,
        # geojson=geojson,
        departure_point=query["start_point"],
        arrival_point=query["end_point"],
        departure_date=int(formated_date.timestamp()),
    )
    # Correct arrival_date based on departure_date

    step.arrival_date = step.departure_date + step.duration_s

    # Add toll price (optional)
    step = ors_add_toll_price(step) if toll_price else step

    ors_journey = TMW.Journey(
        0,
        departure_date=step.departure_date,
        arrival_date=step.arrival_date,
        steps=[step],
    )
    # Add category
    category_journey = list()
    for step in ors_journey.steps:
        if step.type not in [constants.TYPE_TRANSFER, constants.TYPE_WAIT]:
            category_journey.append(step.type)

    ors_journey.category = list(set(category_journey))
    ors_journey.update()
    ors_journey.arrival_date = ors_journey.departure_date + ors_journey.total_duration

    return ors_journey


def ors_gas_price(distance_m, gas_price_eur=1.5, car_consumption=0.0664):
    distance_km = distance_m / 1000
    price_eur = gas_price_eur * (car_consumption * distance_km)
    return price_eur


def ors_add_toll_price(step, toll_price_eur_per_km=0.025):
    distance_km = step.distance_m / 1000
    price_eur = distance_km * toll_price_eur_per_km
    step.price_EUR.append(price_eur)
    return step


@app.task(name="worker", bind=True)
@wrappers.catch(timing=True)
def worker(self, from_loc, to_loc, start_date):
    logger.info("Got request: from={} to={} start={}", from_loc, to_loc, start_date)

    (geoloc_dep, geoloc_arr) = utils.get_points(from_loc=from_loc, to_loc=to_loc)

    query = {
        "start_point": geoloc_dep,
        "end_point": geoloc_arr,
        "departure_date": start_date,
    }
    ors_journey = ors_query_directions(query)

    if ors_journey:
        response = list()
        response.append(ors_journey.to_json())
        return response

    else:
        logger.warning("No ORS journey found")
        return []
