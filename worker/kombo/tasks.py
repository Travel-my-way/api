from loguru import logger
from . import app, logic
from worker import wrappers, utils
from . import global_vars

from .. import constants


@app.task(name="worker", bind=True)
@wrappers.catch(timing=True)
def worker(self, from_loc, to_loc, start_date, nb_passenger):
    logger.info("Got request: from={} to={} start={} nb_passenger={}", from_loc, to_loc,
                start_date, nb_passenger)

    (geoloc_dep, geoloc_arr) = utils.get_points(from_loc=from_loc, to_loc=to_loc)
    # Get cities aroung geolocation
    logger.info("Getting geoloc")
    all_cities = logic.get_cities_from_geo_locs(
        geoloc_dep, geoloc_arr, global_vars["city_db"]
    )
    if not all_cities:
        return list()

    kombo_journeys = logic.compute_kombo_journey(all_cities, int(start_date))

    kombo_json = list()
    id_response = list()

    limit_train = 5
    limit_plane = 2
    limit_coach = 5

    train_journey = [
        journey
        for journey in kombo_journeys
        if constants.TYPE_TRAIN in journey.category
    ]
    coach_journey = [
        journey
        for journey in kombo_journeys
        if constants.TYPE_COACH in journey.category
    ]
    plane_journey = [
        journey
        for journey in kombo_journeys
        if constants.TYPE_PLANE in journey.category
    ]

    for journey in train_journey[0:limit_train]:
        kombo_json.append(journey.to_json())
        id_response.append(journey.id)

    nb_coach = 0
    for journey in coach_journey:
        if (journey.id not in id_response) & (nb_coach < limit_coach):
            kombo_json.append(journey.to_json())
            id_response.append(journey.id)
            nb_coach += 1
    nb_plane = 0
    for journey in plane_journey:
        if (journey.id not in id_response) & (nb_plane < limit_plane):
            kombo_json.append(journey.to_json())
            id_response.append(journey.id)
            nb_plane += 1

    logger.info("Got {} kombo journeys", len(kombo_json))

    return kombo_json
