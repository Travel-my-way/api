import pandas as pd
import copy
import requests
from datetime import datetime as dt, timedelta
from geopy.distance import distance
from loguru import logger
import time

from ..base import BaseWorker
from .. import TMW
from .. import constants
from .. import tmw_api_keys

from worker.kombo import app as kombo
from worker.ors import app as ors


# Get all ferry journeys
def load_ferry_db():
    ferry_db = pd.read_csv('worker/ferries/df_ferry_final_v1.csv')
    ferry_db['date_dep'] = pd.to_datetime(ferry_db['date_dep'])
    ferry_db['date_arr'] = pd.to_datetime(ferry_db['date_arr'])
    return ferry_db


# Get all ferry routes
def load_route_db():
    route_db = pd.read_csv('worker/ferries/ferry_route_db.csv')
    return route_db


# Kombo cities
def update_city_list():
    headers = {
        'token': tmw_api_keys.KOMBO_API_KEY,
    }
    r = requests.get('https://turing.kombo.co/city', headers=headers)
    if r.status_code == 200:
        cities = pd.DataFrame.from_dict(r.json())
        cities['nb_stations'] = cities.apply(lambda x: len(x.stations), axis=1)
        cities = cities[cities['nb_stations'] > 0]
        return cities

    else :
        return None
    # return pd.read_csv('worker/ferries/kombo_cities.csv')


def get_ferries(date_departure, departure_point, arrival_point, ferry_db, routes_db):
    """
    We create a ferry journey based on the ferry database we scraped
    """
    car_journey = ors.ors_query_directions({'start_point' : departure_point,
                                            'end_point': arrival_point,
                                            'departure_date': date_departure.strftime('%Y-%m-%d')})

    # Find relevant routes
    routes = routes_db
    routes['distance_dep_port'] = routes.apply(lambda x: distance([x.lat_clean_dep, x.long_clean_dep],
                                                                  departure_point).m, axis=1)
    routes['distance_port_arrival'] = routes.apply(lambda x: distance([x.lat_clean_arr, x.long_clean_arr],
                                                                      arrival_point).m, axis=1)
    routes['pseudo_distance_total'] = routes.distance_dep_port + routes.distance_port_arrival + routes.distance_m

    if car_journey:
        # only makes sense if the pseudo_distance is lower then the distance by a margin (shortcut by the sea)
        routes['is_relevant'] = routes.apply(lambda x: x.pseudo_distance_total < car_journey.total_distance, axis=1)
        routes = routes[routes.is_relevant]
    else :
        # There are no roads, better try a boat then
        routes['is_relevant'] = True
        routes = routes.sort_values(by='pseudo_distance_total').head(2)
    # Make sure the journey from port_arr to arr is doable in car

    relevant_routes = pd.DataFrame()

    for index, route in routes.iterrows():
        car_journey_dep = ors.ors_query_directions({'start_point': departure_point,
                                                    'end_point': [route.lat_clean_dep,route.long_clean_dep],
                                                    'departure_date': date_departure.strftime('%Y-%m-%d')})
        car_journey_arr = ors.ors_query_directions({'start_point': [route.lat_clean_arr,route.long_clean_arr],
                                                    'end_point': arrival_point,
                                                    'departure_date': date_departure.strftime('%Y-%m-%d')})
        if (car_journey_arr is not None) and (car_journey_dep is not None):
            relevant_routes = relevant_routes.append(route)

    relevant_journeys = pd.DataFrame()

    if len(relevant_routes) > 0:
        relevant_journeys = ferry_db[ferry_db.port_dep.isin(relevant_routes.port_dep) &
                                     ferry_db.port_arr.isin(relevant_routes.port_arr)]

        relevant_journeys['date_dep'] = pd.to_datetime(relevant_journeys.date_dep)

        relevant_journeys = relevant_journeys[relevant_journeys.date_dep > date_departure]
        relevant_journeys = relevant_journeys[relevant_journeys.date_dep < date_departure + timedelta(days=7)]

        relevant_journeys['rankosse'] = relevant_journeys.groupby(['port_dep', 'port_arr'])["date_dep"].rank("dense")
        relevant_journeys = relevant_journeys[relevant_journeys.rankosse < 3]

        if len(relevant_journeys) > 0:
            relevant_journeys['geoloc_port_dep'] = relevant_journeys.apply(lambda x: [x.lat_clean_dep, x.long_clean_dep],
                                                                           axis=1)
            relevant_journeys['geoloc_port_arr'] = relevant_journeys.apply(lambda x: [x.lat_clean_arr, x.long_clean_arr],
                                                                           axis=1)

            return relevant_journeys

    logger.info(f'we found {len(relevant_journeys)} relevant ferry journey')

    return None


def ferry_journey(journeys):

    journey_list = list()

    for index, row in journeys.iterrows():
        distance_m = row.distance_m
        local_emissions = 0
        journey_steps = list()
        journey_step = TMW.Journey_step(0,
                                        _type=constants.TYPE_WAIT,
                                        label=f'Arrive at the port 15 minutes '
                                              f'before departure',
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

        journey_step = TMW.Journey_step(1,
                                        _type=constants.TYPE_FERRY,
                                        label=f'Sail Ferry from {row.port_dep} to {row.port_arr}',
                                        distance_m=distance_m,
                                        duration_s=(row.date_arr - row.date_dep).seconds,
                                        price_EUR=[row.price_clean_ar_eur / 2],
                                        gCO2=local_emissions,
                                        departure_point=[row.lat_clean_dep, row.long_clean_dep],
                                        arrival_point=[row.lat_clean_arr, row.long_clean_arr],
                                        departure_date=int(row.date_dep.timestamp()),
                                        arrival_date=int(row.date_arr.timestamp()),
                                        geojson=[],
                                        )

        journey_steps.append(journey_step)

        journey = TMW.Journey(0,
                              steps=journey_steps,
                              departure_date=journey_steps[0].departure_date,
                              arrival_date=journey_steps[1].arrival_date,
                              )
        journey.total_gCO2 = local_emissions
        journey.category = [constants.TYPE_FERRY]
        journey.booking_link = 'https://www.ferrysavers.co.uk/ferry-routes.htm'
        journey.departure_point = [row.lat_clean_dep, row.long_clean_dep]
        journey.arrival_point = [row.lat_clean_arr, row.long_clean_arr]
        journey.update()
        journey_list.append(journey)

    return journey_list


class FerryWorker(BaseWorker):
    routing_key = "ferry"

    def __init__(self, connection, exchange):
        self.ferry_database = load_ferry_db()
        self.route_database = load_route_db()
        self.city_db = update_city_list()
        super().__init__(connection, exchange)

    def execute(self, message):
        # self.ouibus_database = update_stop_list()
        time_start = time.perf_counter()

        logger.info("Got message: {}", message)
        logger.info("len ferry_db: {}", len(self.ferry_database))
        logger.info("len route_db: {}", len(self.route_database))

        geoloc_dep = message.payload['from'].split(',')
        geoloc_dep[0] = float(geoloc_dep[0])
        geoloc_dep[1] = float(geoloc_dep[1])
        geoloc_arr = message.payload['to'].split(',')
        geoloc_arr[0] = float(geoloc_arr[0])
        geoloc_arr[1] = float(geoloc_arr[1])

        departure_date = dt.strptime(message.payload['start'], '%Y-%m-%d')
        ferry_trips = get_ferries(departure_date,
                                  geoloc_dep,
                                  geoloc_arr,
                                  self.ferry_database,
                                  self.route_database)

        if ferry_trips is None:
            ferry_journeys = list()
        else:
            ferry_journeys = ferry_journey(ferry_trips)
            # pimp ferry journey with kombo calls

            geoloc_port_dep = [ferry_trips.lat_clean_dep.unique()[0],
                               ferry_trips.long_clean_dep.unique()[0]]
            cities_port_dep = kombo.get_cities_from_geo_locs(geoloc_dep, geoloc_port_dep,
                                                             self.city_db, nb_different_city=1)
            geoloc_port_arr = [ferry_trips.lat_clean_arr.unique()[0],
                               ferry_trips.long_clean_arr.unique()[0]]
            cities_port_arr = kombo.get_cities_from_geo_locs(geoloc_port_arr, geoloc_arr,
                                                             self.city_db, nb_different_city=1)

            kombo_dep = kombo.compute_kombo_journey(cities_port_dep, start=departure_date.strftime('%Y-%m-%d'),
                                                    fast_response=True)

            kombo_arr = kombo.compute_kombo_journey(cities_port_arr, start=departure_date.strftime('%Y-%m-%d'),
                                                    fast_response=True)

            additional_journeys = list()
            for journey_ferry in ferry_journeys:
                new_journey = copy.copy(journey_ferry)
                train_found_dep = False
                bus_found_dep = False
                train_found_arr = False
                bus_found_arr = False
                if len(kombo_dep) > 0:
                    for kombo_journey in kombo_dep:
                        if (kombo_journey.category == [constants.TYPE_TRAIN]) & (not train_found_dep):
                            journey_ferry.add_steps(kombo_journey.steps, start_end=True)
                            train_found_dep = True
                        if (kombo_journey.category == [constants.TYPE_COACH]) & (not bus_found_dep):
                            new_journey.add_steps(kombo_journey.steps, start_end=True)
                            bus_found_dep = True
                    # else :
                #     car_journey = ors.ors_query_directions()
                if len(kombo_arr) > 0:
                    for kombo_journey in kombo_arr:
                        if (kombo_journey.category == [constants.TYPE_TRAIN]) & (not train_found_arr):
                            journey_ferry.add_steps(kombo_journey.steps, start_end=False)
                            train_found_arr = True
                        if (kombo_journey.category == [constants.TYPE_COACH]) & (not bus_found_arr):
                            new_journey.add_steps(kombo_journey.steps, start_end=False)
                            bus_found_arr = True

                if (bus_found_arr or bus_found_dep) & (train_found_arr or train_found_dep):
                    additional_journeys.append(new_journey)
                elif(bus_found_arr or bus_found_dep) & (not(train_found_arr or train_found_dep)):
                    # if there is no train, we only
                    journey_ferry = new_journey

            ferry_journeys = ferry_journeys + additional_journeys

        ferry_jsons = list()
        for journey in ferry_journeys:
            ferry_jsons.append(journey.to_json())
        logger.info(f'ici ferry on a envoyé {len(ferry_journeys)} journey en {time.perf_counter()-time_start}')
        return ferry_jsons


