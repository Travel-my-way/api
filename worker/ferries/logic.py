import pandas as pd
from worker import utils, constants, TMW
from loguru import logger

from worker.carbon import emission
from worker.kombo.logic import update_city_list
from worker.ors.tasks import ors_query_directions
from geopy.distance import distance
from datetime import timedelta


# Global init function
def init():
    logger.info("Loading DBs")
    return {
        "ferry_db": load_ferry_db(),
        "route_db": load_route_db(),
        "city_db": update_city_list(),
    }


# Get all ferry journeys
def load_ferry_db():
    ferry_db = pd.read_csv(
        utils.get_relative_file_path(
            base_path=__file__, filename="df_ferry_final_v1.csv"
        )
    )
    ferry_db["date_dep"] = pd.to_datetime(ferry_db["date_dep"])
    ferry_db["date_arr"] = pd.to_datetime(ferry_db["date_arr"])
    return ferry_db


# Get all ferry routes
def load_route_db():
    route_db = pd.read_csv(
        utils.get_relative_file_path(base_path=__file__, filename="ferry_route_db.csv")
    )
    return route_db


def get_ferries(date_departure, departure_point, arrival_point, ferry_db, routes_db, nb_passenger):
    """
    We create a ferry journey based on the ferry database we scraped
    """
    car_journey = ors_query_directions(
        {
            "start_point": departure_point,
            "end_point": arrival_point,
            "departure_date": date_departure.strftime("%Y-%m-%d"),
            "nb_passenger": nb_passenger
        }
    )

    # Find relevant routes
    routes = routes_db
    routes["distance_dep_port"] = routes.apply(
        lambda x: distance([x.lat_clean_dep, x.long_clean_dep], departure_point).m,
        axis=1,
    )
    routes["distance_port_arrival"] = routes.apply(
        lambda x: distance([x.lat_clean_arr, x.long_clean_arr], arrival_point).m, axis=1
    )
    routes["pseudo_distance_total"] = (
        routes.distance_dep_port + routes.distance_port_arrival + routes.distance_m
    )

    if car_journey:
        # only makes sense if the pseudo_distance is lower then the distance by a margin (shortcut by the sea)
        routes["is_relevant"] = routes.apply(
            lambda x: x.pseudo_distance_total < car_journey.total_distance, axis=1
        )
        routes = routes[routes.is_relevant]
    else:
        # There are no roads, better try a boat then
        routes["is_relevant"] = True
        routes = routes.sort_values(by="pseudo_distance_total").head(2)
    # Make sure the journey from port_arr to arr is doable in car

    relevant_routes = pd.DataFrame()

    for index, route in routes.iterrows():
        car_journey_dep = ors_query_directions(
            {
                "start_point": departure_point,
                "end_point": [route.lat_clean_dep, route.long_clean_dep],
                "departure_date": date_departure.strftime("%Y-%m-%d"),
                "nb_passenger": nb_passenger
            }
        )
        car_journey_arr = ors_query_directions(
            {
                "start_point": [route.lat_clean_arr, route.long_clean_arr],
                "end_point": arrival_point,
                "departure_date": date_departure.strftime("%Y-%m-%d"),
                "nb_passenger": nb_passenger
            }
        )
        if (car_journey_arr is not None) and (car_journey_dep is not None):
            relevant_routes = relevant_routes.append(route)

    relevant_journeys = pd.DataFrame()

    if len(relevant_routes) > 0:
        relevant_journeys = ferry_db[
            ferry_db.port_dep.isin(relevant_routes.port_dep)
            & ferry_db.port_arr.isin(relevant_routes.port_arr)
        ]

        relevant_journeys["date_dep"] = pd.to_datetime(relevant_journeys.date_dep)

        relevant_journeys = relevant_journeys[
            relevant_journeys.date_dep > date_departure
        ]
        relevant_journeys = relevant_journeys[
            relevant_journeys.date_dep < date_departure + timedelta(days=7)
        ]

        relevant_journeys["rankosse"] = relevant_journeys.groupby(
            ["port_dep", "port_arr"]
        )["date_dep"].rank("dense")
        relevant_journeys = relevant_journeys[relevant_journeys.rankosse < 3]

        if len(relevant_journeys) > 0:
            relevant_journeys["geoloc_port_dep"] = relevant_journeys.apply(
                lambda x: [x.lat_clean_dep, x.long_clean_dep], axis=1
            )
            relevant_journeys["geoloc_port_arr"] = relevant_journeys.apply(
                lambda x: [x.lat_clean_arr, x.long_clean_arr], axis=1
            )

            return relevant_journeys

    logger.info(f"we found {len(relevant_journeys)} relevant ferry journey")

    return None


def ferry_journey(journeys):

    journey_list = list()

    for index, row in journeys.iterrows():
        distance_m = row.distance_m
        local_emissions = emission.calculate_co2_emissions(
            constants.TYPE_FERRY, distance_m
        )
        journey_steps = list()
        journey_step = TMW.Journey_step(
            0,
            _type=constants.TYPE_WAIT,
            label=f"Arrive at the port 15 minutes " f"before departure",
            distance_m=0,
            duration_s=15 * 60,
            price_EUR=[0],
            gCO2=0,
            departure_point=[row.lat_clean_dep, row.long_clean_dep],
            arrival_point=[row.lat_clean_dep, row.long_clean_dep],
            departure_date=int((row.date_dep - timedelta(seconds=15 * 60)).timestamp()),
            arrival_date=int(row.date_dep.timestamp()),
            geojson=[],
        )
        journey_steps.append(journey_step)

        journey_step = TMW.Journey_step(
            1,
            _type=constants.TYPE_FERRY,
            label=f"Sail Ferry from {row.port_dep} to {row.port_arr}",
            distance_m=distance_m,
            duration_s=(row.date_arr - row.date_dep).seconds,
            price_EUR=[row.price_clean_ar_eur / 2],
            gCO2=local_emissions,
            departure_point=[row.lat_clean_dep, row.long_clean_dep],
            arrival_point=[row.lat_clean_arr, row.long_clean_arr],
            departure_date=int(row.date_dep.timestamp()),
            arrival_date=int(row.date_arr.timestamp()),
            booking_link="https://www.ferrysavers.co.uk/ferry-routes.htm",
            geojson=[],
        )

        journey_steps.append(journey_step)

        journey = TMW.Journey(
            0,
            steps=journey_steps,
            departure_date=journey_steps[0].departure_date,
            arrival_date=journey_steps[1].arrival_date,
        )
        journey.total_gCO2 = local_emissions
        journey.category = [constants.TYPE_FERRY]
        # journey.booking_link = "https://www.ferrysavers.co.uk/ferry-routes.htm"
        journey.departure_point = [row.lat_clean_dep, row.long_clean_dep]
        journey.arrival_point = [row.lat_clean_arr, row.long_clean_arr]
        journey.update()
        journey_list.append(journey)

    return journey_list
