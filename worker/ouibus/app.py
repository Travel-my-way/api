import pandas as pd
import requests
import copy

from datetime import datetime as dt, timedelta
from geopy.distance import distance

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


# Get all bus stations available for OuiBus / Needs to be updated regularly
def update_stop_list():
    headers = {
        'Authorization': 'Token rvZD7TlqePBokwl0T02Onw',
    }
    # Get v1 stops (all actual stops)
    logger.info('start update')
    response = requests.get('https://api.idbus.com/v1/stops', headers=headers)
    logger.info(f'response is {response.status_code}')
    stops_df_v1 = pd.DataFrame.from_dict(response.json()['stops'])
    # Get v2 stops (with meta_station like "Paris - All stations")
    response = requests.get('https://api.idbus.com/v2/stops', headers=headers)
    stops_df_v2 = pd.DataFrame.from_dict(response.json()['stops'])

    # Enrich stops list with meta gare infos
    stops_rich = pandas_explode(stops_df_v2[['id', 'stops']], 'stops')
    stops_rich['stops'] = stops_rich.apply(lambda x: x.stops['id'], axis=1)
    stops_rich = stops_df_v1.merge(stops_rich, how='left', left_on='id', right_on='stops',
                                   suffixes=('', '_meta_gare'))
    # If no meta gare, the id is used
    stops_rich['id_meta_gare'] = stops_rich.id_meta_gare.combine_first(stops_rich.id)
    stops_rich['geoloc'] = stops_rich.apply(lambda x: [x.latitude, x.longitude], axis=1)
    logger.info(f'{stops_rich.shape[0]} Ouibus stops were found, here is an example:\n {stops_rich.sample()}')
    return stops_rich


# Find the stops close to a geo point
def get_stops_from_geo_loc(geoloc_origin, geoloc_destination, ouibus_db, max_distance_km=50):
    """
        This function takes in the departure and arrival points of the TMW journey and returns
            the 3 closest stations within 50 km
    """
    stops_tmp = ouibus_db[(((ouibus_db.latitude-geoloc_origin[0])**2<0.6) &
                           ((ouibus_db.longitude-geoloc_origin[1])**2<0.6)) |
                              (((ouibus_db.latitude-geoloc_destination[0])**2<0.6) &
                               ((ouibus_db.longitude-geoloc_destination[1])**2<0.6))].copy()

    # compute proxi for distance (since we only need to compare no need to take the earth curve into account...)
    stops_tmp['distance_origin'] = stops_tmp.apply(
        lambda x: (x.latitude - geoloc_origin[0])**2 + (x.longitude-geoloc_origin[1])**2, axis=1)
    stops_tmp['distance_destination'] = stops_tmp.apply(
        lambda x: (x.latitude - geoloc_destination[0])**2 + (x.longitude-geoloc_destination[1])**2, axis=1)
    # We get the 5 closests station (within max_distance_km)
    stations = dict()
    stations['origin'] = stops_tmp[stops_tmp.distance_origin < 0.5].sort_values(by='distance_origin').head(5)
    stations['destination'] = stops_tmp[stops_tmp.distance_destination < 0.5].sort_values(by='distance_destination').head(5)
    return stations


def search_for_all_fares(date, origin_id, destination_id, passengers):
    """
    This function takes in all the relevant information for the API call and returns a
        raw dataframe containing all the information from OuiBus API
     """
    # format date as yyy-mm-dd
    date_formated = str(date)[0:10]
    headers = {
        'Authorization': 'Token ' + 'rvZD7TlqePBokwl0T02Onw',
        'Content-Type': 'application/json',
    }
    data = {
        "origin_id": origin_id,
        "destination_id": destination_id,
        "date": date_formated,
        "passengers": passengers
    }
    timestamp = dt.now()
    r = requests.post('https://api.idbus.com/v1/search', headers=headers, data=str(data))
    # print(dt.now() - timestamp)
    try:
        trips = pd.DataFrame.from_dict(r.json()['trips'])
    except:
        return pd.DataFrame()
    trips['departure'] = pd.to_datetime(trips.departure)
    # Let's filter out trips where departure date is before requested time
    trips = trips[trips.departure >= str(date)]
    if not trips.empty:
        return trips
    else:
        # no trips after the requested time, so let's call for the next day
        date_tomorrow = (pd.to_datetime(date) + timedelta(days=1)).date()
        search_for_all_fares(date_tomorrow, origin_id, destination_id, passengers)


def ouibus_journeys(df_response, _id=0):
    """
    This function takes in a DF with detailled info about all the OuiBus trips
    It returns a list of TMW journey objects
        """
    # affect a price to each leg
    df_response = df_response.drop_duplicates(['id', 'arrival', 'departure', 'id_destination', 'id_origin'])
    # df_response.loc[:, 'price_step'] = df_response.apply(lambda x: x['price_cents']/(x['nb_segments']*100), axis=1)
    df_response.insert(1, 'price_step', df_response.apply(lambda x: x['price_cents']/(x['nb_segments']*100), axis=1))
    # Compute distance for each leg
    # print(df_response.columns)
    df_response.insert(1, 'distance_step', df_response.apply(lambda x: distance(x.geoloc_origin_seg, x.geoloc_destination_seg).m,
                                                     axis=1))
    lst_journeys = list()
    # all itineraries :
    # logger.info(f'nb itinerary : {df_response.id.nunique()}')
    for itinerary_id in df_response.id.unique():
        itinerary = df_response[df_response.id == itinerary_id].reset_index(drop=True)
        # boolean to know whether and when there will be a transfer after the leg
        itinerary['next_departure'] = itinerary.departure_seg.shift(-1)
        itinerary['next_stop_name'] = itinerary.short_name_origin_seg.shift(-1)
        itinerary['next_geoloc'] = itinerary.geoloc_origin_seg.shift(-1)
        # get the slugs to create the booking link
        origin_slug = itinerary.origin_slug.unique()[0]
        destination_slug = itinerary.destination_slug.unique()[0]
        i = _id
        lst_sections = list()
        # We add a waiting period at the station of 15 minutes
        step = tmw.Journey_step(i,
                                _type=constants.TYPE_WAIT,
                                label=f'Arrive at the station '
                                      f'{format_timespan(constants.WAITING_PERIOD_OUIBUS)}'
                                      f' before departure',
                                distance_m=0,
                                duration_s=constants.WAITING_PERIOD_OUIBUS,
                                price_EUR=[0],
                                gCO2=0,
                                departure_point=itinerary.geoloc.iloc[0],
                                arrival_point=itinerary.geoloc.iloc[0],
                                departure_date=itinerary.departure_seg[0] - timedelta(seconds=_STATION_WAITING_PERIOD),
                                arrival_date=itinerary.departure_seg[0],
                                geojson=[],
                                )
        lst_sections.append(step)
        i = i + 1
        for index, leg in itinerary.iterrows():
            local_distance_m = leg.distance_step
            local_emissions = 0
            step = tmw.Journey_step(i,
                                    _type=constants.TYPE_COACH,
                                    label=f'Coach OuiBus {leg.bus_number} to {leg.short_name_destination_seg}',
                                    distance_m=local_distance_m,
                                    duration_s=(leg.arrival_seg - leg.departure_seg).seconds,
                                    price_EUR=[leg.price_step],
                                    gCO2=local_emissions,
                                    departure_point=leg.geoloc_origin_seg,
                                    arrival_point=leg.geoloc_destination_seg,
                                    departure_stop_name=leg.short_name_origin_seg,
                                    arrival_stop_name=leg.short_name_destination_seg,
                                    departure_date=leg.departure_seg,
                                    arrival_date=leg.arrival_seg,
                                    trip_code='OuiBus ' + leg.bus_number,
                                    geojson=[],
                                    )
            lst_sections.append(step)
            i = i + 1
            # add transfer steps
            if not pd.isna(leg.next_departure):
                step = tmw.Journey_step(i,
                                        _type=constants.TYPE_TRANSFER,
                                        label=f'Transfer at {leg.short_name_destination_seg}',
                                        distance_m=distance(leg.geoloc_destination_seg,leg.next_geoloc).m,
                                        duration_s=(leg['next_departure'] - leg['arrival_seg']).seconds,
                                        price_EUR=[0],
                                        departure_point=leg.geoloc_destination_seg,
                                        arrival_point=leg.next_geoloc,
                                        departure_stop_name=leg.short_name_destination_seg,
                                        arrival_stop_name=leg.next_stop_name,
                                        gCO2=0,
                                        geojson=[],
                                        )
                lst_sections.append(step)
                i = i + 1
        departure_date_formated = dt.strptime(str(lst_sections[0].departure_date)[0:15],
                                              '%Y-%m-%d %H:%M').strftime('%Y-%m-%d %H:00')
        journey_ouibus = tmw.Journey(_id, steps=lst_sections,
                                     departure_date=lst_sections[0].departure_date,
                                     arrival_date=lst_sections[-1].arrival_date,
                                     booking_link=f'https://fr.ouibus.com/recherche?origin={origin_slug}'
                                                  f'&destination={destination_slug}'
                                                  f'&outboundDate={departure_date_formated}')
        # Add category
        category_journey = list()
        for step in journey_ouibus.steps:
            if step.type not in [constants.TYPE_TRANSFER, constants.TYPE_WAIT]:
                category_journey.append(step.type)

        journey_ouibus.category = list(set(category_journey))
        lst_journeys.append(journey_ouibus)

        # for journey in lst_journeys:
        #    journey.update()

    return lst_journeys


def format_ouibus_response(df_response):
    """
        This function takes in the raw OuiBus API response (previously converted into a DF)
        It return a more enriched dataframe with all the needed information
    """
    # enrich information
    df_response['nb_segments'] = df_response.apply(lambda x: len(x.legs), axis=1)
    df_response['arrival'] = pd.to_datetime(df_response['arrival'])
    df_response['departure'] = pd.to_datetime(df_response['departure'])
    df_response['duration_total'] = df_response.apply(lambda x: (x.arrival - x.departure).seconds, axis=1)
    response_rich = pandas_explode(df_response, 'legs')
    response_rich['origin_id_seg'] = response_rich.apply(lambda x: x['legs']['origin_id'], axis=1)
    response_rich['destination_id_seg'] = response_rich.apply(lambda x: x['legs']['destination_id'], axis=1)
    response_rich['departure_seg'] = pd.to_datetime(response_rich.apply(lambda x: x['legs']['departure'], axis=1))
    response_rich['arrival_seg'] = pd.to_datetime(response_rich.apply(lambda x: x['legs']['arrival'], axis=1))
    response_rich['bus_number'] = response_rich.apply(lambda x: x['legs']['bus_number'], axis=1)
    response_rich = response_rich.merge(_ALL_BUS_STOPS[['id', 'geoloc', 'short_name']], left_on='origin_id_seg',
                                        right_on='id', suffixes=['', '_origin_seg'])
    response_rich = response_rich.merge(_ALL_BUS_STOPS[['id', 'geoloc', 'short_name']], left_on='destination_id_seg',
                                        right_on='id', suffixes=['', '_destination_seg'])
    # filter only most relevant itineraries (2 cheapest + 2 fastest)
    limit = min(2, response_rich.shape[0])
    response_rich = response_rich.sort_values(by='price_cents').head(limit).\
        append(response_rich.sort_values(by='duration_total').head(limit))

    return response_rich


class OuiBusWorker(BaseWorker):

    routing_key = "bus"

    def __init__(self, connection, exchange):
        self.ouibus_database = update_stop_list()
        super().__init__(connection, exchange)

    def execute(self, message):
        # self.ouibus_database = update_stop_list()
        logger.info("Got message: {}", message)
        logger.info("Got message: {}", len(self.ouibus_database))

        geoloc_dep = message.payload['from'].split(',')
        geoloc_dep[0] = float(geoloc_dep[0])
        geoloc_dep[1] = float(geoloc_dep[1])
        geoloc_arr = message.payload['to'].split(',')
        geoloc_arr[0] = float(geoloc_arr[0])
        geoloc_arr[1] = float(geoloc_arr[1])

        all_stops = get_stops_from_geo_loc(geoloc_dep, geoloc_arr, self.ouibus_database)

        logger.info(f'we got the stops ----------')
        origin_meta_gare_ids = all_stops['origin'].id_meta_gare.unique()
        destination_meta_gare_ids = all_stops['destination'].id_meta_gare.unique()
        # Call API for all scenarios
        all_trips = pd.DataFrame()
        for origin_meta_gare_id in origin_meta_gare_ids:
            for destination_meta_gare_id in destination_meta_gare_ids:
                # make sure we don't call the API for a useless trip
                if origin_meta_gare_id != destination_meta_gare_id:
                    all_fares = search_for_all_fares(message.payload['start'], origin_meta_gare_id,
                                                     destination_meta_gare_id,
                                                     [{"id": 1,  "age": 30,  "price_currency": "EUR"}])
                    all_trips = all_trips.append(all_fares)

        # Enrich with stops info
        if all_trips.empty:
            logger.info('no trip found from OuiBus')
            return {"content": "ohai", "demo": 0}

        all_trips = all_trips.merge(self.ouibus_database[['id', 'geoloc', 'short_name']],
                                    left_on='origin_id', right_on='id', suffixes=['', '_origin'])
        all_trips = all_trips.merge(self.ouibus_database[['id', 'geoloc', 'short_name']],
                                    left_on='destination_id', right_on='id', suffixes=['', '_destination'])

        all_trips = format_ouibus_response(all_trips[all_trips.available])
        logger.info(f"ouiouoi busbus")
        logger.info(f"ouibus data base len {len(self.ouibus_database)}")
        return ouibus_journeys(all_trips)

    # Get all bus stations available for OuiBus / Needs to be updated regularly

