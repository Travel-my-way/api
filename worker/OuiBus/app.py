import requests
import os
import pandas as pd
import json
from datetime import datetime as dt, timedelta
from humanfriendly import format_timespan
from geopy.distance import distance
from worker import tmw_api_keys
from worker import TMW as tmw
from worker import constants
from app.co2_emissions import calculate_co2_emissions
import time

pd.set_option('display.max_columns', 999)
pd.set_option('display.width', 1000)

_STATION_WAITING_PERIOD = constants.WAITING_PERIOD_OUIBUS

from loguru import logger

from ..base import BaseWorker


class OuiBusWorker(BaseWorker):

    routing_key = "train"

    def __init__(self):
        self.ouibus_database = load_ouibus_database()
        self.station_waiting_period = constants.WAITING_PERIOD_OUIBUS
        self.passenger= [{"id": 1, "age": 30, "price_currency": "EUR"}]

    def execute(self, message):

        logger.info("Got message: {}", message)
        logger.info(f'length of ouibus_database = {len(self.ouibus_database)}')
        logger.info(f'passenger = {self.passenger}')
        return {"content": "ohai", "demo": 111111}


    # Get all bus stations available for OuiBus from csv
    def load_ouibus_database():
        """
        Load the airport database used to do smart calls to Skyscanner API.
        This DB can be reconstructed thanks to the recompute_airport_database (see end of file)
        """
        path = os.path.join(os.getcwd(), 'data/ouibus_stops.csv')  #
        try:
            logger.info(path)
            bus_station_list = pd.read_csv(path)
            bus_station_list['geoloc'] = bus_station_list.apply(lambda x: [x.latitude, x.longitude], axis=1)
            logger.info('load the ouibus station db. Here is a random example :')
            logger.info(bus_station_list.sample(1))
            return bus_station_list
        except:
            try:
                logger.info(os.path.join(os.getcwd(), 'api/app/', 'data/ouibus_stops.csv'))
                bus_station_list = pd.read_csv(os.path.join(os.getcwd(), 'api/app/', 'data/ouibus_stops.csv'), sep=',')
                bus_station_list['geoloc'] = bus_station_list.apply(lambda x: [x.latitude, x.longitude], axis=1)
                logger.info('load the ouibus station db. Here is a random example :')
                logger.info(bus_station_list.sample(1))
                return bus_station_list
            except:
                logger.info(os.path.join(os.getcwd(), 'app/', 'data/ouibus_stops.csv'))
                bus_station_list = pd.read_csv(os.path.join(os.getcwd(), 'app/', 'data/ouibus_stops.csv'))
                bus_station_list['geoloc'] = bus_station_list.apply(lambda x: [x.latitude, x.longitude], axis=1)
                logger.info('load the ouibus station db. Here is a random example :')
                logger.info(bus_station_list.sample(1))
                return bus_station_list


    # Get all bus stations available for OuiBus / Needs to be updated regularly
    def update_stop_list(retry=0):
        """
            This function loads the DB containing all the Ouibus station we can call
            It calls the OuiBus API, and formats the response into a DF containing
             geolocation amongst other things
         """
        headers = {
            'Authorization': 'Token ' + tmw_api_keys.OUIBUS_API_KEY,
        }
        # Get v1 stops (all actual stops)
        response = requests.get('https://api.idbus.com/v1/stops', headers=headers)
        logger.info(f'response ouibis v1 status {response.status_code}')
        if (response.status_code == 500) & (retry<5):
            logger.warning(f'retry ouibus stop nb {retry+1}')
            return update_stop_list(retry=retry+1)
        stops_df_v1 = pd.DataFrame.from_dict(response.json()['stops'])
        # Get v2 stops (with meta_station like "Paris - All stations")
        response = requests.get('https://api.idbus.com/v2/stops', headers=headers)
        logger.info(f'response ouibis v2 status {response.status_code}')
        if (response.status_code == 500) & (retry<5):
            logger.warning(f'retry ouibus stop nb {retry+1}')
            return update_stop_list(retry=retry+1)
        stops_df_v2 = pd.DataFrame.from_dict(response.json()['stops'])

        # Enrich stops list with meta gare infos
        stops_rich = pandas_explode(stops_df_v2[['id', 'stops', '_carrier_id']], 'stops')
        stops_rich['stops'] = stops_rich.apply(lambda x: x.stops['id'], axis=1)
        stops_rich = stops_df_v1.merge(stops_rich, how='left', left_on='id', right_on='stops',
                                       suffixes=('', '_meta_gare'))
        # If no meta gare, the id is used
        stops_rich['id_meta_gare'] = stops_rich.id_meta_gare.combine_first(stops_rich.id)
        stops_rich['geoloc'] = stops_rich.apply(lambda x: [x.latitude, x.longitude], axis=1)

        logger.info(f'{stops_rich.shape[0]} Ouibus stops were found, here is an example:\n {stops_rich.sample()}')
        stops_rich.to_csv('app/data/ouibus_stops.csv')
        return stops_rich


    # Fonction to call Ouibus API
    def search_for_all_fares(date, origin_id, destination_id, passengers):
        """
        This function takes in all the relevant information for the API call and returns a
            raw dataframe containing all the information from OuiBus API
         """
        # format date as yyy-mm-dd
        date_formated = str(date)[0:10]
        headers = {
            'Authorization': 'Token ' + tmw_api_keys.OUIBUS_API_KEY,
            'Content-Type': 'application/json',
        }
        data = {
            "origin_id": origin_id,
            "destination_id": destination_id,
            "date": date_formated,
            "passengers": passengers
        }
        timestamp = dt.now()
        r = requests.post('https://api.idbus.com/v1/search', headers=headers, data=json.dumps(data))
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


    # Find the stops close to a geo point
    def get_stops_from_geo_loc(geoloc_origin, geoloc_destination, max_distance_km=50):
        """
            This function takes in the departure and arrival points of the TMW journey and returns
                the 3 closest stations within 50 km
        """
        stops_tmp = _ALL_BUS_STOPS[(((_ALL_BUS_STOPS.latitude-geoloc_origin[0])**2<0.6) & ((_ALL_BUS_STOPS.longitude-geoloc_origin[1])**2<0.6)) |
                                  (((_ALL_BUS_STOPS.latitude-geoloc_destination[0])**2<0.6) & ((_ALL_BUS_STOPS.longitude-geoloc_destination[1])**2<0.6))].copy()

        # compute proxi for distance (since we only need to compare no need to take the earth curve into account...)
        stops_tmp['distance_origin'] = stops_tmp.apply(
            lambda x: (x.latitude - geoloc_origin[0])**2 + (x.longitude-geoloc_origin[1])**2, axis=1)
        stops_tmp['distance_destination'] = stops_tmp.apply(
            lambda x: (x.latitude - geoloc_destination[0])**2 + (x.longitude-geoloc_destination[1])**2, axis=1)
        # We get the 5 closests station (within max_distance_km)
        stations = {}
        stations['origin'] = stops_tmp[stops_tmp.distance_origin < 0.5].sort_values(by='distance_origin').head(5)
        stations['destination'] = stops_tmp[stops_tmp.distance_destination < 0.5].sort_values(by='distance_destination').head(5)
        return stations


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
        response_rich = response_rich.sort_values(by='price_cents').head(limit).append(response_rich.sort_values(by='duration_total').head(limit))

        return response_rich


    def compute_trips(date, passengers, geoloc_origin, geoloc_destination):
        """
        Meta Fonction takes a geopoint for departure and arrival,
           1 finds Ouibus status close from departure and arrival
           2 Call API for all meta station of departure and arrival
           3 Returns all available trips
        """
        # Get all stops close to the origin and destination locations
        all_stops = get_stops_from_geo_loc(geoloc_origin, geoloc_destination)
        # Get the meta gare ids to reduce number of request to API
        origin_meta_gare_ids = all_stops['origin'].id_meta_gare.unique()
        destination_meta_gare_ids = all_stops['destination'].id_meta_gare.unique()
        # Call API for all scenarios
        all_trips = pd.DataFrame()
        for origin_meta_gare_id in origin_meta_gare_ids:
            origin_slug = all_stops['origin'][all_stops['origin'].id_meta_gare==origin_meta_gare_id]._carrier_id_meta_gare.unique()[0]
            if pd.isna(origin_slug):
                origin_slug = all_stops['origin'][all_stops['origin'].id_meta_gare==origin_meta_gare_id]._carrier_id.unique()[0]
            for destination_meta_gare_id in destination_meta_gare_ids:
                destination_slug = all_stops['destination'][all_stops['destination'].id_meta_gare == destination_meta_gare_id]._carrier_id_meta_gare.unique()[0]
                if pd.isna(destination_slug):
                    destination_slug = all_stops['destination'][all_stops['destination'].id_meta_gare == destination_meta_gare_id]._carrier_id.unique()[0]
                # logger.info(f'call OuiBus API from {origin_slug} to {destination_slug}')
                # make sure we don't call the API for a useless trip
                if origin_meta_gare_id != destination_meta_gare_id:
                    all_fares = search_for_all_fares(date, origin_meta_gare_id, destination_meta_gare_id, passengers)
                    all_fares['origin_slug'] = origin_slug
                    all_fares['destination_slug'] = destination_slug
                    all_trips = all_trips.append(all_fares)

        # Enrich with stops info
        if all_trips.empty:
            logger.info('no trip found from OuiBus')
            return pd.DataFrame()

        all_trips = all_trips.merge(_ALL_BUS_STOPS[['id', 'geoloc', 'short_name']],
                                    left_on='origin_id', right_on='id', suffixes=['', '_origin'])
        all_trips = all_trips.merge(_ALL_BUS_STOPS[['id', 'geoloc', 'short_name']],
                                    left_on='destination_id', right_on='id', suffixes=['', '_destination'])

        return format_ouibus_response(all_trips[all_trips.available])


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
                                    label=f'Arrive at the station {format_timespan(_STATION_WAITING_PERIOD)} before departure',
                                    distance_m=0,
                                    duration_s=_STATION_WAITING_PERIOD,
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
                local_emissions = calculate_co2_emissions(constants.TYPE_COACH, constants.DEFAULT_CITY,
                                                          constants.DEFAULT_FUEL, constants.DEFAULT_NB_SEATS,
                                                          constants.DEFAULT_NB_KM) *\
                                  constants.DEFAULT_NB_PASSENGERS*local_distance_m
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
            departure_date_formated = dt.strptime(str(lst_sections[0].departure_date)[0:15], '%Y-%m-%d %H:%M').strftime('%Y-%m-%d %H:00')
            journey_ouibus = tmw.Journey(_id, steps=lst_sections,
                                         departure_date=lst_sections[0].departure_date,
                                         arrival_date=lst_sections[-1].arrival_date,
                                         booking_link=f'https://fr.ouibus.com/recherche?origin={origin_slug}&destination={destination_slug}&outboundDate={departure_date_formated}')
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

