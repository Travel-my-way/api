import copy
from datetime import datetime as dt
from loguru import logger
from .. import constants
from . import app, global_vars, logic
from worker import wrappers, utils
from worker.kombo import logic as kombo


@app.task(name="worker", bind=True)
@wrappers.catch(timing=True)
def execute(self, from_loc, to_loc, start_date):

    logger.info("Got request: from={} to={} start={}", from_loc, to_loc, start_date)
    (geoloc_dep, geoloc_arr) = utils.get_points(from_loc=from_loc, to_loc=to_loc)

    departure_date = dt.fromtimestamp(int(start_date))
    ferry_trips = logic.get_ferries(
        departure_date,
        geoloc_dep,
        geoloc_arr,
        global_vars["ferry_db"],
        global_vars["route_db"],
    )

    if ferry_trips is None:
        ferry_journeys = list()
    else:
        ferry_journeys = logic.ferry_journey(ferry_trips)
        # pimp ferry journey with kombo calls

        geoloc_port_dep = [
            ferry_trips.lat_clean_dep.unique()[0],
            ferry_trips.long_clean_dep.unique()[0],
        ]
        cities_port_dep = kombo.get_cities_from_geo_locs(
            geoloc_dep, geoloc_port_dep, global_vars["city_db"], nb_different_city=1
        )
        geoloc_port_arr = [
            ferry_trips.lat_clean_arr.unique()[0],
            ferry_trips.long_clean_arr.unique()[0],
        ]
        cities_port_arr = kombo.get_cities_from_geo_locs(
            geoloc_port_arr, geoloc_arr, global_vars["city_db"], nb_different_city=1
        )

        kombo_dep = kombo.compute_kombo_journey(
            cities_port_dep,
            start=departure_date.timestamp(),
            fast_response=True,
        )

        kombo_arr = kombo.compute_kombo_journey(
            cities_port_arr,
            start=departure_date.timestamp(),
            fast_response=True,
        )

        additional_journeys = list()
        for journey_ferry in ferry_journeys:
            new_journey = copy.copy(journey_ferry)
            train_found_dep = False
            bus_found_dep = False
            train_found_arr = False
            bus_found_arr = False
            if len(kombo_dep) > 0:
                for kombo_journey in kombo_dep:
                    if (kombo_journey.category == [constants.TYPE_TRAIN]) & (
                        not train_found_dep
                    ):
                        journey_ferry.add_steps(kombo_journey.steps, start_end=True)
                        train_found_dep = True
                    if (kombo_journey.category == [constants.TYPE_COACH]) & (
                        not bus_found_dep
                    ):
                        new_journey.add_steps(kombo_journey.steps, start_end=True)
                        bus_found_dep = True
                # else :
            #     car_journey = ors.ors_query_directions()
            if len(kombo_arr) > 0:
                for kombo_journey in kombo_arr:
                    if (kombo_journey.category == [constants.TYPE_TRAIN]) & (
                        not train_found_arr
                    ):
                        journey_ferry.add_steps(kombo_journey.steps, start_end=False)
                        train_found_arr = True
                    if (kombo_journey.category == [constants.TYPE_COACH]) & (
                        not bus_found_arr
                    ):
                        new_journey.add_steps(kombo_journey.steps, start_end=False)
                        bus_found_arr = True

            if (bus_found_arr or bus_found_dep) & (train_found_arr or train_found_dep):
                additional_journeys.append(new_journey)
            elif (bus_found_arr or bus_found_dep) & (
                not (train_found_arr or train_found_dep)
            ):
                # if there is no train, we only
                journey_ferry = new_journey

        ferry_journeys = ferry_journeys + additional_journeys

    ferry_jsons = list()
    for journey in ferry_journeys:
        ferry_jsons.append(journey.to_json())
    logger.info(f"ici ferry on a envoyé {len(ferry_journeys)} journey")
    return ferry_jsons
