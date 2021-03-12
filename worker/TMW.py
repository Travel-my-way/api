"""
INITIATE CLASSES
"""
from datetime import datetime as dt, timedelta
import folium
from threading import Thread
from app import Trainline
from app import Skyscanner
from app import OuiBus
from app import Navitia
from app import ORS
from app import BlablaCar
from app import Ferries
from app import Planes
import time

class Journey:
    def __init__(self, _id, departure_date=dt.now(), arrival_date=dt.now(), booking_link='', steps=[]):
        self.id = _id
        self.category = '' # car/train/plane
        self.label = []
        self.api_list = []
        self.score = 0
        self.total_distance = 0
        self.total_duration = 0
        self.total_price_EUR = 0
        self.total_gCO2 = 0
        self.departure_point = [0, 0]
        self.arrival_point = [0, 0]
        self.departure_date = departure_date
        self.arrival_date  = arrival_date
        self.is_real_journey = True
        self.booking_link = booking_link
        self.bike_friendly = False
        self.steps = steps

    def add(self, steps=[]):
        self.steps.append(steps)
    
    def to_json(self):
        json = {'id': self.id or 0,
                'label': self.label or '',
                'category': self.category or '',
                'score': self.score or '',
                'total_distance': self.total_distance or '',
                'total_duration': self.total_duration or '',
                'total_price_EUR': self.total_price_EUR or '',
                'departure_point': self.departure_point or '',
                'arrival_point': self.arrival_point or '',
                'departure_date': str(self.departure_date) or '',
                'arrival_date': str(self.arrival_date) or '',
                'total_gCO2': self.total_gCO2 or '',
                'is_real_journey': self.is_real_journey or '',
                'booking_link': self.booking_link or '',
                'journey_steps': [step.to_json() for step in self.steps]
                }
        return json

    def reset(self):
        self.score = 0
        self.total_distance = 0
        self.total_duration = 0
        self.total_price_EUR = 0
        self.total_gCO2 = 0
        return self

    def update(self):
        self.score = 0
        self.total_distance = sum(filter(None,[step.distance_m for step in self.steps]))
        self.total_duration = sum(filter(None,[step.duration_s for step in self.steps]))
        self.total_price_EUR = sum(filter(None,[sum(step.price_EUR) for step in self.steps]))
        self.total_gCO2 = sum(filter(None,[step.gCO2 for step in self.steps]))
        self.bike_friendly = all(step.bike_friendly for step in self.steps)
        return self

    def plot_map(self, center=(48.864716,2.349014), tiles = 'Stamen Toner', zoom_start = 4, _map=None):
        _map = init_map(center, zoom_start) if _map == None else _map

        for step in self.steps:
            try:
                step.plot_map(center=center, _map=_map)
            except:
                print('ERROR plot map: step id: {} / type: {}'.format(step.id, step.type))
        return _map

    def add_steps(self, steps_to_add, start_end=True):
        # compute total duration of steps to update arrival and departure times
        additionnal_duration = 0
        for step in steps_to_add:
            additionnal_duration = additionnal_duration + step.duration_s
        # if the steps are added at the beginning of the journey
        if start_end:
            nb_steps_to_add = len(steps_to_add)
            # we update the ids of the steps to preserve the order of the whole journey
            for step_old in self.steps:
                step_old.id = step_old.id + nb_steps_to_add
            self.steps = steps_to_add + self.steps
            self.departure_date = self.departure_date - timedelta(seconds=additionnal_duration)
        # if the steps are at the end of the journey
        else :
            nb_existing_steps = len(self.steps)
            for step_new in steps_to_add:
                step_new.id = step_new.id + nb_existing_steps
            self.steps = self.steps + steps_to_add
            self.arrival_date = self.arrival_date + timedelta(seconds=additionnal_duration)


class Journey_step:
    def __init__(self, _id, _type, label='', distance_m=0, duration_s=0, price_EUR=[0.0], gCO2 = 0, departure_point=[0.0],
                 arrival_point=[0.0], departure_stop_name='', arrival_stop_name='', departure_date=dt.now()
                 , arrival_date=dt.now(), bike_friendly=False, transportation_final_destination='', trip_code='', geojson=''):
        self.id = _id
        self.type = _type
        self.label = label
        self.distance_m = distance_m
        self.duration_s = duration_s
        self.price_EUR = price_EUR
        self.gCO2 = gCO2
        self.departure_point = departure_point
        self.arrival_point = arrival_point
        self.departure_stop_name = departure_stop_name
        self.arrival_stop_name = arrival_stop_name
        self.departure_date = departure_date
        self.arrival_date = arrival_date
        self.trip_code = trip_code #AF350 / TGV8342 / Métro Ligne 2 ect...
        self.bike_friendly = bike_friendly
        self.transportation_final_destination = transportation_final_destination # Direction of metro / final stop on train ect..
        self.geojson = geojson

    def to_json(self):
        json = {'id': self.id or '',
                'type': self.type or '',
                'label': self.label or '',
                'distance_m': self.distance_m or '',
                'duration_s': self.duration_s or '',
                'price_EUR': self.price_EUR or '',
                'departure_point': self.departure_point or '',
                'arrival_point': self.arrival_point or '',
                'departure_stop_name': self.departure_stop_name or '',
                'arrival_stop_name': self.arrival_stop_name or '',
                'departure_date': str(self.departure_date) or '',
                'arrival_date': str(self.arrival_date) or '',
                'trip_code': self.trip_code or '',
                'gCO2': self.gCO2 or '',
                # 'geojson': self.geojson,
                }
        return json

    def plot_map(self, center=(48.864716,2.349014), zoom_start=4, _map=None):
        _map = init_map(center, zoom_start) if _map == None else _map

        folium.features.GeoJson(data=self.geojson,
                                name=self.label,
                                overlay=True).add_to(_map)
        return _map

class Query:
    def __init__(self, _id, start_point, end_point, departure_date=None):
        self.id = _id
        self.start_point = start_point
        self.end_point = end_point
        self.departure_date = departure_date        # example of format (based on navitia): 20191012T063700

    def to_json(self):
        json = {'id': self.id,
                 'start': [round(self.start_point[0],4), round(self.start_point[1],4)],
                 'end': [round(self.end_point[0],4), round(self.end_point[1],4)],
                 'departure_date': str(self.departure_date),
                }
        return json

    def plot_navitia_coverage(self, center=(48.864716,2.349014), zoom_start = 4,_map=None):
        _map = init_map(center, zoom_start) if _map == None else _map

        _map = self.start_point.plot_navitia_coverage(_map=_map)
        _map = self.end_point.plot_navitia_coverage(_map=_map)
        return _map

class Point:
    def __init__(self, address, near=False):
        self.address = address
        self.coord = geocode_address(address)  # [lon,lat]
        self.navitia = self.navitia_coverage(self.coord[0], self.coord[1]) # [lon,lat]
        self.near_flag = near
        if near == True:
            self.near_airports = None # TO BE COMPLETED --> [point, point...]
            self.near_train_stations = None # TO BE COMPLETED --> [point, point...]
            self.near_bus_stations = None # TO BE COMPLETED --> [point, point...]

    def to_json(self):
        json =  {
                    'address':self.address,
                    'coord':self.coord,
                    'navitia':self.navitia,
                }
        if self.near_flag == True:
            json['near_airports'] = self.near_airports
            json['near_train_stations'] = self.near_train_stations
            json['near_bus_stations'] = self.near_bus_stations
        return json

    def navitia_coverage(self, lon, lat):
        coverage = navitia_coverage_gpspoint(lon, lat)
        if coverage == False:
            return False

        cov_json = {
            'name':coverage['regions'][0]['id'],
            'polygon':coverage['regions'][0]['shape'],
        }
        return cov_json

    def plot_navitia_coverage(self, center=(48.864716,2.349014), zoom_start = 4,_map=None):
        _map = init_map(center, zoom_start) if _map == None else _map

        if self.navitia != False:
            folium.vector_layers.Polygon(locations=self.navitia['polygon'],
                                tooltip=self.navitia['name'],
                                ).add_to(_map)
            folium.map.Marker(location=self.coord[::-1],
                                tooltip=self.address).add_to(_map)
        return _map


class ThreadComputeJourney(Thread):
    """
    The class helps parallelize the computation journeys
    """
    def __init__(self, api, query, distance_car=None):
        Thread.__init__(self)
        self._return = None
        self.api = api
        self.query = query
        self.distance_car = distance_car
        self.run_time = 0

    def run(self):
        if self.api == 'OuiBus':
            time_launch = time.perf_counter()
            journeys = OuiBus.main(self.query)
            self.run_time = time.perf_counter() - time_launch
        elif self.api == 'Skyscanner':
            time_launch = time.perf_counter()
            journeys = Skyscanner.main(self.query)
            self.run_time = time.perf_counter() - time_launch
        elif self.api == 'Planes':
            time_launch = time.perf_counter()
            journeys = Planes.main(self.query)
            self.run_time = time.perf_counter() - time_launch
        elif self.api == 'Trainline':
            time_launch = time.perf_counter()
            journeys = Trainline.main(self.query)
            self.run_time = time.perf_counter() - time_launch
        elif self.api == 'ORS':
            time_launch = time.perf_counter()
            journeys = ORS.ORS_query_directions(self.query)
            self.run_time = time.perf_counter() - time_launch
        elif self.api == 'BlaBlaCar':
            time_launch = time.perf_counter()
            journeys = BlablaCar.main(self.query)
            self.run_time = time.perf_counter() - time_launch
        elif self.api == 'Ferry':
            time_launch = time.perf_counter()
            journeys = Ferries.main(self.query, self.distance_car)
            self.run_time = time.perf_counter() - time_launch
        else:
            time_launch = time.perf_counter()
            journeys = list()
            self.run_time = time.perf_counter() - time_launch
        self._return = journeys

    def join(self):
        Thread.join(self)
        return self._return, self.run_time


class ThreadNavitiaCall(Thread):
    """
    The class helps parallelize the computation journeys
    """
    def __init__(self, query):
        Thread.__init__(self)
        self._return = None
        self.query = query
        self.run_time = 0

    def run(self):
        journey = Navitia.navitia_query_directions(self.query)
        if journey is None:
            # If Navitia could not give a response, we ask ors to do a car trip
            journey = list()
            journey.append(ORS.ORS_query_directions(self.query))
        self._return = journey

    def join(self):
        Thread.join(self)
        return self._return, self.query


"""
BASIC FUNCTIONS
"""


def init_map(center, zoom_start, tiles = 'Stamen Toner'):
        map_params = {'tiles':tiles,
              'location':center,
              'zoom_start': zoom_start}
        _map = folium.Map(**map_params)
        return _map


def geocode_address(address):
    '''
    address: string
    coord : [lon, lat]
    '''
    ORS_client = start_ORS_client()
    lon, lat = ORS_client.pelias_search(address,size=1)['features'][0]['geometry']['coordinates']
    return lon, lat


def get_CO2(travel_type, distance, param={}):
    # Calculate CO2 emissions based on travel_type and distance
    # Import csv database (ADEME)
    # param = {col:value}
    """
    import pandas as pd
    EF_filepath = 'EmissionFactor.csv'
    df_EF = pd.read_csv(EF_filepath,sep=';')
    
    for col in param.keys():
        df_EF = df_EF[df_EF[col] == param[col]]

    print(df_EF)
    print('ERROR get_CO2() --> param variable did not ') if df_EF.size!=1 else True
    EF = df_EF[value]
    """

    dict_EF = {             # Emision factor (EF)
        'walk':0.0,
        'wait':0.0,
        'car':0.255,
        'bus':0.167,
        'metro':0.006,
        'tram':0.006,
        'train':0.037,
        'TGV':0.00369,
        'plane':0.23,
    }
    try:
        EF = dict_EF[travel_type]
    except:
        print('ERROR: travel_type "{}" is not listed in Emission Factor.'.format(travel_type))
        print('Returning 0.0 kgCO2/passenger')
        return 0
    emission = EF * distance
    return emission






