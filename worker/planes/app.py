import pandas as pd
from datetime import datetime as dt
from geopy.distance import distance
from loguru import logger
import time

from ..base import BaseWorker
from .. import TMW
from .. import constants


# Get all plane journeys
def load_plane_db():
    plane_db = pd.read_csv('worker/planes/df_planes_transfert.csv')
    return plane_db


# Get all plane routes
def load_airport_db():
    airport_db = pd.read_csv('worker/planes/skyscanner_europe_airport_list.csv')
    airport_db['geoloc'] = airport_db.apply(lambda x: [x.latitude, x.longitude], axis=1)
    return airport_db


# Find the stops close to a geo point
def get_cities_from_geo_locs(geoloc_dep, geoloc_arrival, airport_db, nb_different_city=2):
    """
        This function takes in the departure and arrival points of the TMW journey and returns
            the 3 closest stations within 50 km
    """
    # We filter only on the closest stops to have a smaller df (thus better perfs)
    stops_tmp = airport_db[(((airport_db.latitude-geoloc_dep[0])**2<0.6) & ((airport_db.longitude-geoloc_dep[1])**2<0.6)) |
                              (((airport_db.latitude-geoloc_arrival[0])**2<0.6) &
                               ((airport_db.longitude-geoloc_arrival[1])**2<0.6))].copy()

    stops_tmp['distance_dep'] = stops_tmp.apply(lambda x: (geoloc_dep[0]- x.latitude)**2 +
                                                          (geoloc_dep[1]- x.longitude)**2, axis =1)
    stops_tmp['distance_arrival'] = stops_tmp.apply(lambda x: (geoloc_arrival[0]- x.latitude)**2 +
                                                              (geoloc_arrival[1]- x.longitude)**2, axis =1)

    # We get all station within approx 55 km (<=> 0.5 of distance proxi)
    parent_station_id_list = dict()

    # We keep only the 2 closest cities from dep and arrival
    parent_station_id_list['origin'] = stops_tmp[stops_tmp.distance_dep < 0.5].sort_values(
                    by='distance_dep').head(nb_different_city).Code
    parent_station_id_list['arrival'] = stops_tmp[stops_tmp.distance_arrival < 0.5].sort_values(
        by='distance_arrival').head(nb_different_city).Code
    return parent_station_id_list


def compute_plane_journey(all_airports, plane_db):
    all_trips = pd.DataFrame()

    for origin_airport in all_airports['origin']:
        for arrival_airport in all_airports['arrival']:
            all_trips = all_trips.append(get_planes(origin_airport, arrival_airport, plane_db))

    if len(all_trips) == 0:
        logger.warning('pas de all_trips')
        return None

    return all_trips


def get_planes(origin_airport, arrival_airport, plane_db):
    """
    We create a plane journey based on the plane database we scraped
    """

    # We search for direct liaison
    relevant_journeys = plane_db[(plane_db.airport_from_first == origin_airport) &
                                 (plane_db.airport_to_first == arrival_airport)]

    if len(relevant_journeys) > 0:
        relevant_journeys = relevant_journeys[['flight_number_first', 'airport_from_first', 'airport_to_first',
                                      'latitude_dep_first', 'longitude_dep_first', 'latitude_arr_first',
                                      'longitude_arr_first', 'distance_m_first']]
        relevant_journeys.columns = ['flight_number', 'airport_from', 'airport_to', 'latitude_dep', 'longitude_dep',
                                     'latitude_arr', 'longitude_arr', 'distance_m']
        relevant_journeys['nb_step'] = 1
        return relevant_journeys

    else :
        # We search for liaison with transfert
        relevant_journeys = plane_db[(plane_db.airport_from_first == origin_airport) &
                                     (plane_db.airport_to_sec == arrival_airport)]

        if len(relevant_journeys) > 0:
            relevant_journeys['nb_step'] = 2
            return relevant_journeys
        else:
            return None


def plane_journey(plane_trips):

    journey_list = list()
    logger.info(plane_trips)
    logger.info(type(plane_trips))
    for index, row in plane_trips.iterrows():
        if row.nb_step == 1:
            # Direct Flights
            distance_m = row.distance_m
            local_emissions = 0
            journey_steps = list()
            journey_step = TMW.Journey_step(0,
                                            _type=constants.TYPE_PLANE,
                                            label=f'Plane {row.flight_number} from {row.airport_from} to {row.airport_to}',
                                            distance_m=distance_m,
                                            duration_s=0,
                                            price_EUR=[0],
                                            gCO2=local_emissions,
                                            departure_point=[row.latitude_dep, row.longitude_dep],
                                            arrival_point=[row.latitude_arr, row.longitude_arr],
                                            departure_date=0,
                                            arrival_date=0,
                                            geojson=[],
                                            )

            journey_steps.append(journey_step)

            journey = TMW.Journey(0,
                                  steps=journey_steps,
                                  departure_date=journey_steps[0].departure_date,
                                  arrival_date=journey_steps[0].arrival_date,
                                  )
            journey.total_gCO2 = local_emissions
            journey.category = [constants.TYPE_PLANE]
            journey.booking_link = ''
            journey.departure_point = [row.latitude_dep, row.longitude_dep]
            journey.arrival_point = [row.latitude_arr, row.longitude_arr]
            journey.update()
            journey_list.append(journey)

        elif row.nb_step == 2:
            # Trip with transfert
            local_emissions = 0
            journey_steps = list()
            # first leg
            journey_step = TMW.Journey_step(0,
                                            _type=constants.TYPE_PLANE,
                                            label=f'Plane {row.flight_number_first} from {row.airport_from_first} '
                                                  f'to {row.airport_to_first}',
                                            distance_m=row.distance_m_first,
                                            duration_s=0,
                                            price_EUR=[0],
                                            gCO2=local_emissions,
                                            departure_point=[row.latitude_dep_first, row.longitude_dep_first],
                                            arrival_point=[row.latitude_arr_first, row.longitude_arr_first],
                                            departure_date=0,
                                            arrival_date=0,
                                            geojson=[],
                                            )

            journey_steps.append(journey_step)

            # second leg
            journey_step = TMW.Journey_step(0,
                                            _type=constants.TYPE_PLANE,
                                            label=f'Plane {row.flight_number_sec} from {row.airport_from_sec} '
                                                  f'to {row.airport_to_sec}',
                                            distance_m=row.distance_m_sec,
                                            duration_s=0,
                                            price_EUR=[0],
                                            gCO2=local_emissions,
                                            departure_point=[row.latitude_dep_sec, row.longitude_dep_sec],
                                            arrival_point=[row.latitude_arr_sec, row.longitude_arr_sec],
                                            departure_date=0,
                                            arrival_date=0,
                                            geojson=[],
                                            )
            journey_steps.append(journey_step)

            journey = TMW.Journey(0,
                                  steps=journey_steps,
                                  departure_date=journey_steps[0].departure_date,
                                  arrival_date=journey_steps[1].arrival_date,
                                  )
            journey.total_gCO2 = local_emissions
            journey.category = [constants.TYPE_PLANE]
            journey.booking_link = ''
            journey.departure_point = [row.latitude_dep_first, row.longitude_dep_first]
            journey.arrival_point = [row.latitude_arr_sec, row.longitude_arr_sec]
            journey.update()
            journey_list.append(journey)

    return journey_list


def ultra_fake_plane_journey(geoloc_dep, geoloc_arr):
    journey_list = list()
    distance_m = distance(geoloc_dep, geoloc_arr).m
    local_emissions = 0
    journey_steps = list()
    journey_step = TMW.Journey_step(0,
                                    _type=constants.TYPE_PLANE,
                                    label=f'Ultra fake plane from {geoloc_dep} to {geoloc_arr}',
                                    distance_m=distance_m,
                                    duration_s=0,
                                    price_EUR=[0],
                                    gCO2=local_emissions,
                                    departure_point=geoloc_dep,
                                    arrival_point=geoloc_arr,
                                    departure_date=0,
                                    arrival_date=0,
                                    geojson=[],
                                    )

    journey_steps.append(journey_step)

    journey = TMW.Journey(0,
                          steps=journey_steps,
                          departure_date=journey_steps[0].departure_date,
                          arrival_date=journey_steps[0].arrival_date,
                          )
    journey.total_gCO2 = local_emissions
    journey.category = [constants.TYPE_PLANE]
    journey.booking_link = ''
    journey.departure_point = geoloc_dep
    journey.arrival_point = geoloc_arr
    journey.update()

    journey_list.append(journey)
    return journey_list


class PlaneWorker(BaseWorker):
    routing_key = "plane"

    def __init__(self, connection, exchange):
        self.airport_database = load_airport_db()
        self.plane_database = load_plane_db()
        super().__init__(connection, exchange)

    def execute(self, message):
        # self.ouibus_database = update_stop_list()
        time_start = time.perf_counter()
        logger.info("Got message: {}", message)
        logger.info("len airport_db: {}", len(self.airport_database))
        logger.info("len plane_db: {}", len(self.plane_database))

        geoloc_dep = message.payload['from'].split(',')
        geoloc_dep[0] = float(geoloc_dep[0])
        geoloc_dep[1] = float(geoloc_dep[1])
        geoloc_arr = message.payload['to'].split(',')
        geoloc_arr[0] = float(geoloc_arr[0])
        geoloc_arr[1] = float(geoloc_arr[1])

        departure_date = dt.strptime(message.payload['start'], '%Y-%m-%d')

        airports = get_cities_from_geo_locs(geoloc_dep, geoloc_arr, self.airport_database, nb_different_city=2)

        plane_trips = compute_plane_journey(airports, self.plane_database)

        if plane_trips is not None:
            plane_journeys = plane_journey(plane_trips)
        else:
            plane_journeys = ultra_fake_plane_journey(geoloc_dep, geoloc_arr)

        plane_jsons = list()
        for journey in plane_journeys:
            plane_jsons.append(journey.to_json())
        logger.info(f'ici planes on a envoyé {len(plane_journeys)} journey en {time.perf_counter()-time_start}')

        return plane_jsons


