import pandas as pd
from datetime import datetime as dt, timedelta
from geopy.distance import distance
from loguru import logger

from ..base import BaseWorker
from .. import TMW


# Get all ports
def load_port_db():
    port_db = pd.read_csv('worker/ferries/df_port_ferries_final_v1.csv')
    port_db['geoloc'] = port_db.apply(lambda x: [x.lat_clean, x.long_clean], axis=1)
    return port_db


# Get all ferry journeys
def load_ferry_db():
    ferry_db = pd.read_csv('worker/ferries/df_ferry_final_v1.csv')
    ferry_db['date_dep'] = pd.to_datetime(ferry_db['date_dep'])
    ferry_db['date_arr'] = pd.to_datetime(ferry_db['date_arr'])
    return ferry_db


# Find the ports close to geo points
def get_ports_from_geo_locs(geoloc_dep, geoloc_arrival, port_db):
    """
    This function takes in the departure and arrival points of the TMW journey and returns
        the most relevant corresponding Skyscanner cities to build a plane journey
    We first look at the closest airport, if the city is big enough we keep only one city,
        if not we add the next closest airport and if this 2nd city is big enough we keep only the two first
        else we look at the 3rd and final airports
    """
    stops_tmp = port_db.copy()
    # compute proxi for distance (since we only need to compare no need to take the earth curve into account...)
    stops_tmp['distance_dep'] = stops_tmp.apply(lambda x: (x.lat_clean - geoloc_dep[0]) ** 2 +
                                                          (x.long_clean - geoloc_dep[1]) ** 2, axis=1)
    stops_tmp['distance_arrival'] = stops_tmp.apply(lambda x: (x.lat_clean - geoloc_arrival[0]) ** 2 +
                                                              (x.long_clean - geoloc_arrival[1]) ** 2, axis=1)

    # We get the 5 closest ports for departure and arrival
    port_deps = stops_tmp.sort_values(by='distance_dep').head(5)
    port_arrs = stops_tmp.sort_values(by='distance_arrival').head(5)
    return port_deps, port_arrs


def get_ferries(date_departure, departure_point, arrival_point, ferry_db, port_db):
    """
    We create a ferry journey based on the ferry database we scraped
    """
    # Find relevant ports
    port_deps, port_arrs = get_ports_from_geo_locs(departure_point, arrival_point, port_db)

    print(port_deps)
    print(port_arrs)
    # Find journeys
    journeys = ferry_db[(ferry_db.port_dep.isin(port_deps.port_clean.unique())) &
                        ferry_db.port_arr.isin(port_arrs.port_clean.unique())]

    journeys['date_dep'] = pd.to_datetime(journeys.date_dep)
    journeys = journeys[journeys.date_dep > date_departure]
    journeys = journeys[journeys.date_dep < date_departure + timedelta(days=7)]

    if len(journeys) == 0:
        logger.info(f'No ferry journey was found')
        return None

    journeys['distance_dep_port'] = journeys.apply(lambda x: distance([x.lat_clean_dep, x.long_clean_dep],
                                                                      departure_point).m, axis=1)
    journeys['distance_port_arrival'] = journeys.apply(lambda x: distance([x.lat_clean_arr, x.long_clean_arr],
                                                                          arrival_point).m, axis=1)
    journeys['pseudo_distance_total'] = journeys.distance_dep_port + journeys.distance_port_arrival + \
                                        journeys.distance_m

    journeys['rankosse'] = journeys.groupby(['port_dep', 'port_arr'])["date_dep"].rank("dense")
    journeys = journeys[journeys.rankosse < 3]

    if len(journeys) == 0:
        return list()

    journey_list = list()

    for index, row in journeys.iterrows():
        distance_m = row.distance_m
        local_emissions = 0
        journey_steps = list()
        journey_step = TMW.Journey_step(0,
                                        _type='Wait',
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
                                        _type='ferry',
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
        journey.category = 'Ferry'
        journey.booking_link = 'https://www.ferrysavers.co.uk/ferry-routes.htm'
        journey.departure_point = [row.lat_clean_dep, row.long_clean_dep]
        journey.arrival_point = [row.lat_clean_arr, row.long_clean_arr]
        journey.update()
        journey_list.append(journey)

    return journey_list


class FerryWorker(BaseWorker):
    routing_key = "ferry"

    def __init__(self, connection, exchange):
        self.port_database = load_port_db()
        self.ferry_database = load_ferry_db()
        super().__init__(connection, exchange)

    def execute(self, message):
        # self.ouibus_database = update_stop_list()

        logger.info("Got message: {}", message)
        logger.info("len port_db: {}", len(self.port_database))
        logger.info("len ferry_db: {}", len(self.ferry_database))

        geoloc_dep = message.payload['from'].split(',')
        geoloc_dep[0] = float(geoloc_dep[0])
        geoloc_dep[1] = float(geoloc_dep[1])
        geoloc_arr = message.payload['to'].split(',')
        geoloc_arr[0] = float(geoloc_arr[0])
        geoloc_arr[1] = float(geoloc_arr[1])

        departure_date = dt.strptime(message.payload['start'], '%Y-%M-%d')
        ferry_journeys = get_ferries(departure_date,
                                     geoloc_dep,
                                     geoloc_arr,
                                     self.ferry_database,
                                     self.port_database)

        if ferry_journeys is None :
            return list()
        ferry_jsons = list()
        for journey in ferry_journeys:
            ferry_jsons.append(journey.to_json())

        return ferry_jsons


