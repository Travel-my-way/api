import pandas as pd
from loguru import logger
import os
from .. import constants

# DEFAULT VALUES
DEFAULT_CITY = ('', '', '')
DEFAULT_NB_PASSENGERS = 1
DEFAULT_FUEL = ''
DEFAULT_NB_SEATS = ('', '')

# HEADERS
TYPE_OF_TRANSPORT = "subcategory_3"
CITY = 'city'
CITY_SIZE_MIN = 'city_size_min'
CITY_SIZE_MAX = 'city_size_max'
NB_SEATS_MIN = "capacity_min"
NB_SEATS_MAX = "capacity_max"
NB_KM_MIN = "distance_min"
NB_KM_MAX = "distance_max"
FUEL = "fuel"

CARBON_DF_PATH = 'worker/carbon/emission.csv'


def calculate_co2_emissions(type_transport, distance_m, type_city=None, fuel=None, nb_seats=None):
    # read csv
    carbon_df = pd.read_csv(CARBON_DF_PATH, delimiter=',')
    carbon_df = carbon_df[carbon_df[TYPE_OF_TRANSPORT] == type_transport]

    if type_city is not None:
        carbon_df = carbon_df[carbon_df[CITY] == type_city[0]]
        filter_city_size = (carbon_df.loc[:, CITY_SIZE_MIN] >= float(type_city[1])) &\
                           (carbon_df.loc[:, CITY_SIZE_MAX] < float(type_city[2]))
        carbon_df = carbon_df[filter_city_size]

    # For Planes we differentiate for court, moyen and long courrier
    if type_transport == constants.TYPE_PLANE:
        carbon_df = carbon_df[(carbon_df[NB_KM_MIN] < distance_m/1000) & (carbon_df[NB_KM_MAX] > distance_m/1000)]

    if fuel is None:
        # We take the avg value then
        carbon_df = carbon_df[(carbon_df[FUEL] == 'avg') | (pd.isna(carbon_df[FUEL]))]
    else:
        carbon_df = carbon_df[carbon_df[FUEL] == fuel]

    # The result will be in grams of CO2
    return carbon_df['value'].mean() * distance_m
