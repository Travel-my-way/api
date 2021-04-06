"""
INITIATE CLASSES
"""
from datetime import datetime as dt

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
                'score': self.score or 0,
                'total_distance': self.total_distance or 0,
                'total_duration': self.total_duration or 0,
                'total_price_EUR': self.total_price_EUR or '',
                'departure_point': self.departure_point or '',
                'arrival_point': self.arrival_point or '',
                'departure_date': int(self.departure_date) or 0,
                'arrival_date': int(self.arrival_date) or 0,
                'total_gCO2': self.total_gCO2 or 0,
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
        if len(self.steps)>0:
            self.departure_point = self.steps[0].departure_point
            self.arrival_point = self.steps[-1].arrival_point
            self.departure_date = self.steps[0].departure_date
            self.arrival_date = self.steps[-1].arrival_date
        return self

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
            self.departure_date = self.departure_date - additionnal_duration
        # if the steps are at the end of the journey
        else :
            nb_existing_steps = len(self.steps)
            for step_new in steps_to_add:
                step_new.id = step_new.id + nb_existing_steps
            self.steps = self.steps + steps_to_add
            self.arrival_date = self.arrival_date + additionnal_duration


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
        # Direction of metro / final stop on train ect..
        self.transportation_final_destination = transportation_final_destination
        self.geojson = geojson

    def to_json(self):
        json = {'id': self.id or 0,
                'type': self.type or '',
                'label': self.label or '',
                'distance_m': self.distance_m or 0,
                'duration_s': self.duration_s or 0,
                'price_EUR': self.price_EUR or '',
                'departure_point': self.departure_point or '',
                'arrival_point': self.arrival_point or '',
                'departure_stop_name': self.departure_stop_name or '',
                'arrival_stop_name': self.arrival_stop_name or '',
                'departure_date': int(self.departure_date) or '',
                'arrival_date': int(self.arrival_date) or '',
                'trip_code': self.trip_code or '',
                'gCO2': self.gCO2 or 0,
                # 'geojson': self.geojson,
                }
        return json


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
