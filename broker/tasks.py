import json
from loguru import logger
from datetime import datetime as dt
from . import app
from broker import wrappers
from . import Navitia
from . import TMW
from worker.ors.tasks import ors_query_directions


def recreate_journey_objects(results_list, id_journey=0):
    # # logger.info('into recreate_journey_objects')
    # # logger.info(results_list)
    journey_list = list()
    for result in results_list:
        # logger.info(result)
        journey_list.append(json_to_journey(result, id_journey))
        id_journey += 1

    return journey_list, id_journey


def json_to_journey(json_journey, id_journey):
    step_list = list()
    # logger.info('into json_to_journey')
    # logger.info(type(json_journey))
    # logger.info(json_journey)
    id_journey_step = 0
    for step in json_journey["journey_steps"]:
        step_list.append(
            TMW.Journey_step(
                id_journey_step,
                _type=step["type"],
                label=step["label"],
                distance_m=float(step["distance_m"]),
                duration_s=float(step["duration_s"]),
                price_EUR=step["price_EUR"],
                departure_point=step["departure_point"],
                arrival_point=step["arrival_point"],
                departure_stop_name=step["departure_stop_name"],
                arrival_stop_name=step["arrival_stop_name"],
                departure_date=int(step["departure_date"]),
                arrival_date=int(step["arrival_date"]),
                gCO2=float(step["gCO2"]),
                booking_link=step["booking_link"],
                # bike_friendly=step['bike_friendly'],
            )
        )
        id_journey_step += 1
    journey = TMW.Journey(
        id_journey,
        steps=step_list,
        departure_date=int(json_journey["departure_date"]),
        arrival_date=int(json_journey["arrival_date"]),
        booking_link=json_journey["booking_link"],
    )

    journey.category = json_journey["category"]
    return journey


@app.task(name="broker", bind=True)
@wrappers.catch(timing=True)
def compute_results(results: list, from_loc: str, to_loc: str, start_date: str, nb_passenger: str) -> list:
    logger.info("Got request: from={} to={} start={} nb_passenger={}", from_loc, to_loc,
                start_date, nb_passenger)

    # Extract successful results from lists
    partials = [r for r in results if r["status"] == "success"]
    # logger.info(partials)
    content = {}

    journey_list = list()

    geoloc_dep = from_loc.split(",")
    geoloc_dep[0] = float(geoloc_dep[0])
    geoloc_dep[1] = float(geoloc_dep[1])
    geoloc_arr = to_loc.split(",")
    geoloc_arr[0] = float(geoloc_arr[0])
    geoloc_arr[1] = float(geoloc_arr[1])

    id_journey = 0
    for o in partials:
        try:
            journey_to_add, id_journey = recreate_journey_objects(
                o["result"], id_journey
            )
            journey_list = journey_list + journey_to_add
            id_journey += 1
        except Exception as e:
            logger.error(
                "recreate_journey_objects a foiré pour {}: {}", o["emitter"], e
            )

        worker_name = o["worker"]
        logger.info("Got {} journeys from {}", len(o["result"]), worker_name)
        content[worker_name] = o["result"]

    urban_queries = list()
    urban_queries_json = list()
    logger.info(f"on a {len(journey_list)} journey")
    for interurban_journey in journey_list:
        if len(interurban_journey.steps[0].departure_point) == 1:
            logger.warning("mauvais trip")
            pass
        query_dep = TMW.Query(
            0,
            geoloc_dep,
            interurban_journey.steps[0].departure_point,
            int(start_date),
        )
        if query_dep.to_json() not in urban_queries_json:
            urban_queries.append(query_dep)
            urban_queries_json.append(query_dep.to_json())
        query_arr = TMW.Query(
            0,
            interurban_journey.steps[-1].arrival_point,
            geoloc_arr,
            int(start_date),
        )

        if query_arr.to_json() not in urban_queries_json:
            urban_queries.append(query_arr)
            urban_queries_json.append(query_arr.to_json())

    # Deduplicate queries
    # urban_queries = list(set(urban_queries))
    logger.info(f"Got {len(urban_queries)} urban queries")

    urban_journey_dict = dict()
    departure_point_name = "Départ"
    arrival_point_name = "Arrivé"
    for urban_query in urban_queries:
        urban_journey = Navitia.navitia_query_directions(urban_query)

        if urban_journey is None and urban_query.start_point != urban_query.end_point:
            urban_journey = list()
            urban_journey.append(
                ors_query_directions(
                    {
                        "start_point": urban_query.start_point,
                        "end_point": urban_query.end_point,
                        "departure_date": int(urban_query.departure_date),
                        "nb_passenger": nb_passenger
                    },
                    avoid_ferries=False,
                )
            )
        elif urban_journey is not None:
            if urban_query.start_point == geoloc_dep:
                departure_point_name = urban_journey[0].steps[0].departure_stop_name
            elif urban_query.end_point == geoloc_arr:
                arrival_point_name = urban_journey[0].steps[-1].arrival_stop_name

        urban_journey_dict[str(urban_query.to_json())] = urban_journey

    for interurban_journey in journey_list:
        json_key_start = TMW.Query(
            0,
            geoloc_dep,
            interurban_journey.steps[0].departure_point,
            int(start_date),
        ).to_json()
        start_to_station_steps = urban_journey_dict[str(json_key_start)]
        json_key_end = TMW.Query(
            0,
            interurban_journey.steps[-1].arrival_point,
            geoloc_arr,
            int(start_date),
        ).to_json()
        station_to_arrival_steps = urban_journey_dict[str(json_key_end)]

        if (start_to_station_steps is not None) & (
            station_to_arrival_steps is not None
        ):
            if (start_to_station_steps[0] is not None) & (
                station_to_arrival_steps[0] is not None
            ):
                if (len(start_to_station_steps[0].steps) > 0) & (
                        len(station_to_arrival_steps[0].steps) > 0
                ):
                    # interurban_journey.add_steps(start_to_station_steps[0].steps, start_end=True)
                    # interurban_journey.add_steps(station_to_arrival_steps[0].steps, start_end=False)
                    start_to_station_steps[0].update()
                    station_to_arrival_steps[0].update()
                    interurban_journey.add_journey_as_steps(
                        start_to_station_steps[0], start_end=True
                    )
                    interurban_journey.add_journey_as_steps(
                        station_to_arrival_steps[0], start_end=False
                    )

        interurban_journey.update()
        # create stop names for ors
        for step_nb in range(len(interurban_journey.steps)):
            step = interurban_journey.steps[step_nb]
            if (step.departure_stop_name is None) or (step.departure_stop_name == ""):
                if step_nb == 0:
                    step.departure_stop_name = departure_point_name
                else :
                    step.departure_stop_name = interurban_journey.steps[step_nb-1].arrival_stop_name
                if step_nb == len(interurban_journey.steps)-1:
                        step.arrival_stop_name = arrival_point_name
                else:
                    step.arrival_stop_name = interurban_journey.steps[step_nb+1].departure_stop_name

    response = list()

    for journey in journey_list:
        # response[journey.id] = journey.to_json()
        response.append(journey.to_json())
    logger.info(response)
    return response
