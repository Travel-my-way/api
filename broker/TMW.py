"""
INITIATE CLASSES
"""
from datetime import datetime as dt
from . import constants

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
                'journey_steps': self.jsonify_steps()
                }
        return json

    def jsonify_steps(self):
        # to filter out steps we don't want to display in the front
        tmp = list()
        for step in self.steps:
            if step.type not in [constants.TYPE_WAIT, constants.TYPE_TRANSFER]:
                tmp.append(step.to_json())
        return tmp
        #return [step.to_json() for step in self.steps]


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
            self.category = list(set((filter(lambda x: x in [constants.TYPE_TRAIN, constants.TYPE_PLANE,constants.TYPE_COACH,
                                                             constants.TYPE_FERRY, constants.TYPE_CARPOOOLING, constants.TYPE_CAR],
                                             [step.type for step in self.steps]))))
            if self.category == list():
                self.category = list(set((filter(lambda x: x in [constants.TYPE_BUS, constants.TYPE_BIKE, constants.TYPE_METRO,
                                                 constants.TYPE_TRAM], [step.type for step in self.steps]))))
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

    def add_journey_as_steps(self, journey_to_add, start_end=True):
        # This function is meant to be used only for intra_urban journey
        # compute total duration of steps to update arrival and departure times
        additionnal_duration = journey_to_add.total_duration
        # create pseudo journey_step from journey
        if len(journey_to_add.category) > 0:
            # we take the first category to randomly display only one
            journey_step_type = journey_to_add.category[0]
        else:
            journey_step_type = constants.TYPE_WALK
        pseudo_step = Journey_step(0,
                                   _type=journey_step_type,
                                   label='',
                                   distance_m=journey_to_add.total_distance,
                                   duration_s=journey_to_add.total_duration,
                                   price_EUR=[journey_to_add.total_price_EUR],
                                   gCO2=journey_to_add.total_gCO2,
                                   departure_point= journey_to_add.departure_point,
                                   arrival_point= journey_to_add.arrival_point,
                                   departure_stop_name= journey_to_add.steps[0].departure_stop_name,
                                   arrival_stop_name= journey_to_add.steps[-1].arrival_stop_name,
                                   departure_date= journey_to_add.steps[0].departure_date,
                                   arrival_date= journey_to_add.steps[-1].arrival_date
                                )
        # if the steps are added at the beginning of the journey
        if start_end:
            pseudo_step.label = f'Tranport entre point de départ {journey_to_add.steps[0].departure_stop_name} ' \
                                f'et {self.steps[0].departure_stop_name}'
            pseudo_step.departure_date = self.departure_date - pseudo_step.duration_s
            pseudo_step.arrival_date = self.departure_date
            # we update the ids of the steps to preserve the order of the whole journey
            for step_old in self.steps:
                step_old.id = step_old.id + 1
            self.steps.insert(0, pseudo_step)
            self.departure_date = self.departure_date - additionnal_duration
        # if the steps are at the end of the journey
        else :
            pseudo_step.label = f'Tranport entre {self.steps[-1].arrival_stop_name} et' \
                                f' arrivé au {journey_to_add.steps[-1].arrival_stop_name} '
            pseudo_step.departure_date = self.arrival_date
            pseudo_step.arrival_date = self.arrival_date + pseudo_step.duration_s
            nb_existing_steps = len(self.steps)
            pseudo_step.id = nb_existing_steps
            self.steps.append(pseudo_step)
            self.arrival_date = self.arrival_date + additionnal_duration


class Journey_step:
    def __init__(self, _id, _type, label='', distance_m=0, duration_s=0, price_EUR=[0.0], gCO2 = 0, departure_point=[0.0],
                 arrival_point=[0.0], departure_stop_name='', arrival_stop_name='', departure_date=dt.now()
                 , arrival_date=dt.now(), bike_friendly=False, transportation_final_destination='', booking_link='', trip_code='', geojson=''):
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
        self.booking_link = booking_link
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
                'departure_date': int(self.departure_date) or 0,
                'arrival_date': int(self.arrival_date) or 0,
                'trip_code': self.trip_code or '',
                'gCO2': self.gCO2 or 0,
                'booking_link': self.booking_link or '',
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
                 'start_point': [round(self.start_point[0],4), round(self.start_point[1],4)],
                 'end_point': [round(self.end_point[0],4), round(self.end_point[1],4)],
                 'departure_date': str(self.departure_date),
                }
        return json
