import pandas as pd
import requests
import copy

from datetime import datetime as dt, timedelta

from loguru import logger

from ..base import BaseWorker

from .. import TMW as tmw
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


# function to get all trainline fares and trips
def search_for_trips(date, start_point, end_point):
    """
        This function takes in all the relevant information for the API call and returns a
            dataframe containing all the information from BlaBlaCar API
         """
    # round date  to next hour to adapt to api's behavior
    url = 'https://public-api.blablacar.com/api/v3/trips'

    # formated_date = date + timedelta(seconds=3599)
    formated_date = dt.strptime(date, '%Y-%m-%d')
    formated_date = dt.strftime(formated_date, '%Y-%m-%dT%H:%m:%S')
    params = { 'key' : '4RiVIJL2JNqgR9qH2ISI4afoRZ9Humgx',
        'from_coordinate' : str(start_point[0]) + ',' + str(start_point[1]),
         'to_coordinate' : str(end_point[0]) + ',' + str(end_point[1]),
         'currency' : 'EUR',
         'locale' : 'fr-FR',
         'start_date_local': formated_date,
         'count' : 10
         }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        return format_blablacar_response(response.json(), date, start_point, end_point)
    else :
        print(response.json())
        return None


# Fucntion to format the trainline json repsonse
def format_blablacar_response(rep_json, departure_date, start_point, end_point):
    try:
        trips = pd.DataFrame.from_dict(rep_json['trips'])
    except:
        logger.warning(f'Call to BlaBlaCar API gave no response for date {departure_date} from {start_point}'
                       f' to {end_point}, json response was {rep_json}')

    if trips.empty:
        return None
    # Get price in euro
    trips['price'] = trips.apply(lambda x: x['price']['amount'], axis=1)
    # De jsonize waypoints
    # Add trip_id
    trips['trip_id'] = trips.index
    trips_waypoints = pandas_explode(trips, 'waypoints')

    # Get info from each waypoint
    trips_waypoints['date_time'] = trips_waypoints.apply(lambda x: x['waypoints']['date_time'], axis=1)
    trips_waypoints['date_time'] =  pd.to_datetime(trips_waypoints['date_time'])
    trips_waypoints['city'] = trips_waypoints.apply(lambda x: x['waypoints']['place']['city'], axis=1)
    trips_waypoints['address'] = trips_waypoints.apply(lambda x: x['waypoints']['place']['address'], axis=1)
    trips_waypoints['latitude'] = trips_waypoints.apply(lambda x: x['waypoints']['place']['latitude'], axis=1)
    trips_waypoints['longitude'] = trips_waypoints.apply(lambda x: x['waypoints']['place']['longitude'], axis=1)

    return trips_waypoints


# Fucntion to format the trainline json repsonse
def blablacar_journey(df_response):
    """
        This function takes in a DF with detailled info about all the BlaBlaCar trips
        It returns a list of TMW journey objects    """

    lst_journeys = list()
    # all itineraries :
    # print(f'nb itinerary : {df_response.id_global.nunique()}')
    _id = 0
    for trip_id in df_response.trip_id.unique():
        itinerary = df_response[df_response.trip_id == trip_id]
        # Get the arrival info on the same line
        itinerary['date_time_arrival'] = itinerary.date_time.shift(-1)
        itinerary['city_arrival'] = itinerary.city.shift(-1)
        itinerary['address_arrival'] = itinerary.address.shift(-1)
        itinerary['latitude_arrival'] = itinerary.latitude.shift(-1)
        itinerary['longitude_arrival'] = itinerary.longitude.shift(-1)

        # boolean to know whether and when there will be a transfer after the leg
        itinerary['next_departure'] = itinerary.date_time.shift(1)

        # Get rid of the "last line" for the last leg of the blablacar trip
        itinerary = itinerary[~pd.isna(itinerary.city_arrival)]

        # Divide price between legs weighted by distance and distance
        itinerary['total_distance'] = itinerary.distance_in_meters.sum()
        itinerary['price'] = float(itinerary['price'])
        itinerary['price_leg'] = itinerary.apply(lambda x: x.distance_in_meters / x.total_distance * x.price, axis=1)

        i = _id
        lst_sections = list()
        # We add a waiting period at the pick up point of 15 minutes
        step = tmw.Journey_step(i,
                                _type=constants.TYPE_WAIT,
                                label=f'Arrive at pick up point'
                                      f' {constants.WAITING_PERIOD_BLABLACAR} '
                                      f'minutes before departure',
                                distance_m=0,
                                duration_s=constants.WAITING_PERIOD_BLABLACAR,
                                price_EUR=[0],
                                gCO2=0,
                                departure_point=[itinerary.latitude.iloc[0], itinerary.longitude.iloc[0]],
                                arrival_point=[itinerary.latitude.iloc[0], itinerary.longitude.iloc[0]],
                                departure_date=itinerary.date_time.iat[0] -
                                timedelta(seconds=constants.WAITING_PERIOD_BLABLACAR),
                                arrival_date=itinerary.date_time.iat[0],
                                bike_friendly=False,
                                geojson=[],
                                )

        lst_sections.append(step)
        i = i + 1
        # Go through all steps of the journey
        for index, leg in itinerary.iterrows():
            local_distance_m = leg.distance_in_meters
            local_emissions = 0
            step = tmw.Journey_step(i,
                                    _type=constants.TYPE_CARPOOOLING,
                                    label=f'BlablaCar trip from {leg.city} to {leg.city_arrival}',
                                    distance_m=local_distance_m,
                                    duration_s=leg.duration_in_seconds,
                                    price_EUR=[leg.price_leg],
                                    gCO2=local_emissions,
                                    departure_point=[leg.latitude, leg.longitude],
                                    arrival_point=[leg.latitude_arrival, leg.longitude_arrival],
                                    departure_stop_name=leg.address + ' ' + leg.city,
                                    arrival_stop_name=leg.address_arrival + ' ' + leg.city_arrival,
                                    departure_date=leg.date_time,
                                    arrival_date=leg.date_time_arrival,
                                    trip_code='BlaBlaCar_' + str(leg.trip_id),
                                    bike_friendly=False,
                                    geojson=[],
                                    )
            lst_sections.append(step)
            i = i + 1
            # add transfer steps
            if not pd.isna(leg.next_departure):
                step = tmw.Journey_step(i,
                                        _type=constants.TYPE_TRANSFER,
                                        label=f'Transfer at {leg.name_arrival_seg}',
                                        distance_m=0,
                                        duration_s=(leg['next_departure'] - leg['arrival_date_seg']).seconds,
                                        price_EUR=[0],
                                        departure_point=[leg.latitude_arrival, leg.longitude_arrival],
                                        arrival_point=[leg.latitude_arrival, leg.longitude_arrival],
                                        departure_stop_name=leg.address_arrival + ' ' + leg.city_arrival,
                                        arrival_stop_name=leg.address_arrival + ' ' + leg.city_arrival,
                                        departure_date=leg.date_time_arrival,
                                        arrival_date=leg.next_departure,
                                        gCO2=0,
                                        bike_friendly=False,
                                        geojson=[],
                                        )
                lst_sections.append(step)
                i = i + 1
        journey_blablacar = tmw.Journey(_id, steps=lst_sections,
                                        departure_date=lst_sections[0].departure_date,
                                        arrival_date=lst_sections[-1].arrival_date,
                                        booking_link=leg.link)
        # Add category
        category_journey = list()
        for step in journey_blablacar.steps:
            if step.type not in [constants.TYPE_TRANSFER, constants.TYPE_WAIT]:
                category_journey.append(step.type)

        journey_blablacar.category = list(set(category_journey))
        lst_journeys.append(journey_blablacar)

    return lst_journeys


class BlaBlaCarWorker(BaseWorker):

    routing_key = "covoiturage"

    def execute(self, message):
        # self.ouibus_database = update_stop_list()
        logger.info("Got message: {}", message)

        geoloc_dep = message.payload['from'].split(',')
        geoloc_dep[0] = float(geoloc_dep[0])
        geoloc_dep[1] = float(geoloc_dep[1])
        geoloc_arr = message.payload['to'].split(',')
        geoloc_arr[0] = float(geoloc_arr[0])
        geoloc_arr[1] = float(geoloc_arr[1])

        all_trips = search_for_trips(message.payload['start'], geoloc_dep, geoloc_arr)

        # logger.info(f'len the trips {len(all_trips)}')
        if not all_trips:
            return {"content": "no blalabla trips were found ", "demo": 0}

        blablacar_journeys = blablacar_journey(all_trips)
        blablacar_json = list()
        for journey in blablacar_journeys:
            blablacar_json.append(journey.to_json())

        return blablacar_json


