from loguru import logger
import requests
import pandas as pd
from datetime import datetime as dt, timedelta
import copy
import time
from geopy.distance import distance

from ..base import BaseWorker

from .. import TMW as tmw
from .. import tmw_api_keys
from .. import constants


def pandas_explode(df, column_to_explode):
    """
    Similar to Hive's EXPLODE function, take a column with iterable elements, and flatten the iterable to one element
    per observation in the output table

    :param df: A dataframe to explod
    :type df: pandas.DataFrame
    :param column_to_explode:
    :type column_to_explode: str
    :return: An exploded data frame
    :rtype: pandas.DataFrame
    """

    # Create a list of new observations
    new_observations = list()

    # Iterate through existing observations
    for row in df.to_dict(orient='records'):

        # Take out the exploding iterable
        explode_values = row[column_to_explode]
        del row[column_to_explode]

        # Create a new observation for every entry in the exploding iterable & add all of the other columns
        for explode_value in explode_values:

            # Deep copy existing observation
            new_observation = copy.deepcopy(row)

            # Add one (newly flattened) value from exploding iterable
            new_observation[column_to_explode] = explode_value

            # Add to the list of new observations
            new_observations.append(new_observation)

    # Create a DataFrame
    return_df = pd.DataFrame(new_observations)

    # Return
    return return_df


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


# Find the stops close to a geo point
def get_cities_from_geo_locs(geoloc_dep, geoloc_arrival, city_db):
    """
        This function takes in the departure and arrival points of the TMW journey and returns
            the 3 closest stations within 50 km
    """
    # We filter only on the closest stops to have a smaller df (thus better perfs)
    stops_tmp = city_db[(((city_db.latitude-geoloc_dep[0])**2<0.6) & ((city_db.longitude-geoloc_dep[1])**2<0.6)) |
                              (((city_db.latitude-geoloc_arrival[0])**2<0.6) &
                               ((city_db.longitude-geoloc_arrival[1])**2<0.6))].copy()

    stops_tmp['distance_dep'] = stops_tmp.apply(lambda x: (geoloc_dep[0]- x.latitude)**2 +
                                                          (geoloc_dep[1]- x.longitude)**2, axis =1)
    stops_tmp['distance_arrival'] = stops_tmp.apply(lambda x: (geoloc_arrival[0]- x.latitude)**2 +
                                                              (geoloc_arrival[1]- x.longitude)**2, axis =1)

    # We get all station within approx 55 km (<=> 0.5 of distance proxi)
    parent_station_id_list = dict()

    # We keep only the 2 closest cities from dep and arrival
    parent_station_id_list['origin'] = stops_tmp[stops_tmp.distance_dep < 0.5].sort_values(
                    by='distance_dep').head(2).id
    parent_station_id_list['arrival'] = stops_tmp[stops_tmp.distance_arrival < 0.5].sort_values(
        by='distance_arrival').head(2).id
    return parent_station_id_list


def enrich_trips(trips, stations, companies):
    trips_exploded = pandas_explode(pandas_explode(trips, 'segments'), 'segments')
    trips_grouped = trips_exploded.groupby('tripId').agg({'segments': 'count'})
    trips_exploded[['departureStationId', 'arrivalStationId', 'departureTime', 'arrivalTime', 'companyId']] \
        = trips_exploded.apply(extract_from_segments, axis=1)
    trips_exploded = trips_exploded.merge(stations, left_on='departureStationId', right_on='id', suffixes=['', '_dep'])
    trips_exploded = trips_exploded.merge(stations, left_on='arrivalStationId', right_on='id', suffixes=['', '_arr'])
    trips_exploded = trips_exploded.merge(companies, left_on='companyId', right_on='id', suffixes=['', '_comp'])
    trips_exploded = trips_exploded.merge(trips_grouped, on='tripId', suffixes=['', '_nb'])
    trips_exploded['departureTime'] = pd.to_datetime(trips_exploded['departureTime'])
    trips_exploded['arrivalTime'] = pd.to_datetime(trips_exploded['arrivalTime'])

    return trips_exploded


def dataframize(json_object):
    tmp_list = list()
    for key in json_object:
        tmp_list.append(json_object[key])
    return pd.DataFrame(tmp_list)


def extract_from_segments(x):
    return pd.Series([x['segments']['departureStationId'], x['segments']['arrivalStationId'], x['segments']['departureTime'], x['segments']['arrivalTime'], x['segments']['companyId']])


def extract_from_stations(x):
    return pd.Series([x['id']['name'], x['latitude']['longitude'], x['cityId']['departureTime'], x['segments']['arrivalTime'], x['segments']['companyId']])


def search_kombo(id_dep, id_arr, date, nb_passengers=1):
    headers = {
        'token': tmw_api_keys.KOMBO_API_KEY,
    }
    r = requests.get(f'https://turing.kombo.co/search/{id_dep}/{id_arr}/{date}/{nb_passengers}', headers=headers)
    logger.info(f'https://turing.kombo.co/search/{id_dep}/{id_arr}/{date}/{nb_passengers}')
    if r.status_code == 200:
        pollkey = r.json()['key']
        # print('paul k ok')
    else:
        print(f'error in search {r.status_code}')
        return pd.DataFrame()
    time.sleep(0.5)
    response = requests.get(f'https://turing.kombo.co/pollSearch/{pollkey}', headers=headers)
    logger.info('tout va bien wesh')
    trips = pd.DataFrame.from_dict(response.json()['trips'])
    logger.info('tout va bien wesh wesh')
    stations = dataframize(response.json()['dependencies']['stations'])
    logger.info('tout va bien wesh wesh wesh')
    companies = dataframize(response.json()['dependencies']['companies'])
    while not response.json()['completed']:
        time.sleep(0.5)
        response = requests.get(f'https://turing.kombo.co/pollSearch/{pollkey}', headers=headers)
        trips = trips.append(pd.DataFrame.from_dict(response.json()['trips']))
        stations = stations.append(dataframize(response.json()['dependencies']['stations']))
        companies = companies.append(dataframize(response.json()['dependencies']['companies']))
    if len(trips) == 0:
        logger.info('trips est vide')
        return pd.DataFrame()
    stations = stations.drop_duplicates()
    companies = companies.drop_duplicates()
    print(trips.shape)
    print(stations.shape)
    print(companies.shape)

    trips_clean = enrich_trips(trips, stations, companies)
    return trips_clean


def kombo_journey(df_response, passengers=1):
    """
        This function takes in a DF with detailled info about all the Kombo trips
        It returns a list of TMW journey objects
    """
    # affect a price to each leg (otherwise we would multiply the price by the number of legs
    logger.info('hekjvba:kqbvrkb:qkvtebrb')
    logger.info(df_response)
    logger.info(df_response.columns)
    logger.info(df_response.shape)
    df_response['price_step'] = df_response.price / (df_response.segments_nb * 100)

    # Compute distance for each leg
    # print(df_response.columns)
    df_response['distance_step'] = df_response.apply(
        lambda x: distance([x.latitude, x.longitude], [x.latitude_arr, x.longitude_arr]).m,
        axis=1)

    lst_journeys = list()
    tranportation_mean_wait = {
        'bus': 15 * 60,
        'train': 15 * 60,
        'flight': 90 * 60,
    }
    # all itineraries :
    # print(f'nb itinerary : {df_response.id_global.nunique()}')
    for tripId in df_response.tripId.unique():
        itinerary = df_response[df_response.tripId == tripId].reset_index(drop=True)
        # boolean to know whether and when there will be a transfer after the leg
        itinerary['next_departure'] = itinerary.departureTime.shift(-1)
        itinerary['next_stop_name'] = itinerary.name.shift(-1)
        itinerary['next_latitude'] = itinerary.latitude.shift(-1)
        itinerary['next_longitude'] = itinerary.longitude.shift(-1)

        i = 0
        lst_sections = list()

        station_wait = tranportation_mean_wait[itinerary.transportType.iloc[0]]
        # We add a waiting period at the station of 15 minutes
        step = tmw.Journey_step(i,
                            _type='Wait',
                            label=f'Arrive at the station {station_wait} before departure',
                            distance_m=0,
                            duration_s=station_wait,
                            price_EUR=[0],
                            gCO2=0,
                            departure_point=[itinerary.latitude.iloc[0], itinerary.longitude.iloc[0]],
                            arrival_point=[itinerary.latitude.iloc[0], itinerary.longitude.iloc[0]],
                            departure_date=itinerary.departureTime[0] - timedelta(seconds=station_wait),
                            arrival_date=itinerary.departureTime[0],
                            bike_friendly=False,
                            geojson=[],
                            )

        lst_sections.append(step)
        i = i + 1
        # Go through all steps of the journey
        for index, leg in itinerary.iterrows():
            local_distance_m = distance([leg.latitude, leg.longitude], [leg.latitude_arr, leg.longitude_arr]).m
            local_transportation_type = leg.transportType
            local_emissions = 0
            step = tmw.Journey_step(i,
                                _type=local_transportation_type,
                                label=f'{local_transportation_type} from {leg.name} to {leg.name_arr}',
                                distance_m=local_distance_m,
                                duration_s=(leg.arrivalTime - leg.departureTime).seconds,
                                price_EUR=[leg.price_step],
                                gCO2=local_emissions,
                                departure_point=[leg.latitude, leg.longitude],
                                arrival_point=[leg.latitude_arr, leg.longitude_arr],
                                departure_stop_name=leg.name,
                                arrival_stop_name=leg.name_arr,
                                departure_date=leg.departureTime,
                                arrival_date=leg.arrivalTime,
                                trip_code='',
                                bike_friendly=False,
                                geojson=[],
                                )
            lst_sections.append(step)
            i = i + 1
            # add transfer steps
            if not pd.isna(leg.next_departure):
                step = tmw.Journey_step(i,
                                    _type='transfert',
                                    label=f'Transfer at {leg.name_arr}',
                                    distance_m=0,
                                    duration_s=(leg['next_departure'] - leg['arrivalTime']).seconds,
                                    price_EUR=[0],
                                    departure_point=[leg.latitude_arr, leg.longitude_arr],
                                    arrival_point=[leg.next_latitude, leg.next_longitude],
                                    departure_stop_name=leg.name_arr,
                                    arrival_stop_name=leg.next_stop_name,
                                    departure_date=leg.arrivalTime,
                                    arrival_date=leg.next_departure,
                                    gCO2=0,
                                    bike_friendly=True,
                                    geojson=[],
                                    )
                lst_sections.append(step)
                i = i + 1
        journey_train = tmw.Journey(0, steps=lst_sections,
                                departure_date=lst_sections[0].departure_date,
                                arrival_date=lst_sections[-1].arrival_date,
                                booking_link=f'www.kombo.co/affilate/1/fr/{passengers}/{leg.tripId}')
        # Add category
        # category_journey = list()
        # for step in journey_train.steps:
        #    if step.type not in [constants.TYPE_TRANSFER, constants.TYPE_WAIT]:
        #        category_journey.append(step.type)

        # journey_train.category = list(set(category_journey))
        lst_journeys.append(journey_train)

        # for journey in lst_journeys:
        #    journey.update()

    return lst_journeys


class KomboWorker(BaseWorker):

    routing_key = "kombo"

    def __init__(self, connection, exchange):
        self.city_db = update_city_list()
        super().__init__(connection, exchange)

    def execute(self, message):

        logger.info("Got message: {}", message)

        geoloc_dep = message.payload['from'].split(',')
        geoloc_dep[0] = float(geoloc_dep[0])
        geoloc_dep[1] = float(geoloc_dep[1])
        geoloc_arr = message.payload['to'].split(',')
        geoloc_arr[0] = float(geoloc_arr[0])
        geoloc_arr[1] = float(geoloc_arr[1])

        start =  message.payload['start']

        all_cities = get_cities_from_geo_locs(geoloc_dep, geoloc_arr, self.city_db)
        if not all_cities:
            return {"content": "no trip found on Kombo", "demo": 0}

        all_trips = pd.DataFrame()
        logger.info(all_cities)
        i = 0
        for origin_city in all_cities['origin']:
            for arrival_city in all_cities['arrival']:
                logger.info(f'tout va bien {i}')
                all_trips = all_trips.append(search_kombo(origin_city, arrival_city, start))
                time.sleep(1)

        logger.info('avant le test de all_trips')
        time.sleep(2)
        if len(all_trips) == 0:
            logger.info('pas de all_trips')
            return {"content": "no trip found on Kombo", "demo": 0}
        logger.info('still here')
        kombo_journeys = kombo_journey(all_trips)

        kombo_json = list()
        for journey in kombo_journeys:
            kombo_json.append(journey.to_json())

        return kombo_json
