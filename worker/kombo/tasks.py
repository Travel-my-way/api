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

    for journey in kombo_journeys:
        kombo_json.append(journey.to_json())

    logger.info("Got {} kombo journeys", len(kombo_json))

    return kombo_json
