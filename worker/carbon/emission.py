import pandas as pd
from pathlib import Path
from loguru import logger

# DEFAULT VALUES
DEFAULT_CITY = ("", "", "")
DEFAULT_NB_PASSENGERS = 1
DEFAULT_FUEL = ""
DEFAULT_NB_SEATS = ("", "")

# HEADERS
TYPE_OF_TRANSPORT = "subcategory_3"
CITY = "city"
CITY_SIZE_MIN = "city_size_min"
CITY_SIZE_MAX = "city_size_max"
NB_SEATS_MIN = "capacity_min"
NB_SEATS_MAX = "capacity_max"
NB_KM_MIN = "distance_min"
NB_KM_MAX = "distance_max"
FUEL = "fuel"

# Path relative to current file,update if necessary
CARBON_DF_PATH = Path(__file__).parent.absolute() / "emission.csv"

# Carbon dataframe,let's singleton it !
carbon_frame = None


def init_carbon():
    global carbon_frame
    carbon_frame = pd.read_csv(CARBON_DF_PATH, delimiter=",")
    logger.debug("Loaded global carbon frame.")


def get_carbon_frame_for_transport(transport_type: str) -> pd.DataFrame:
    global carbon_frame
    if carbon_frame is None:
        logger.debug("First time carbon frame loading")
        carbon_frame = pd.read_csv(CARBON_DF_PATH, delimiter=",")

    # Return filtered frame
    logger.debug("Returning carbon frame for {} type", transport_type)
    return carbon_frame[carbon_frame[TYPE_OF_TRANSPORT] == transport_type]


def calculate_co2_emissions(
    type_transport, nb_km, type_city=None, fuel=None, nb_seats=None
):
    carbon_df = get_carbon_frame_for_transport(transport_type=type_transport)

    if type_city is not None:
        carbon_df = carbon_df[carbon_df[CITY] == type_city[0]]
        filter_city_size = (carbon_df.loc[:, CITY_SIZE_MIN] >= float(type_city[1])) & (
            carbon_df.loc[:, CITY_SIZE_MAX] < float(type_city[2])
        )
        carbon_df = carbon_df[filter_city_size]

    if fuel is not None:
        carbon_df = carbon_df[carbon_df[FUEL] == fuel]

    if nb_seats is not None:
        filter_nb_seats = (carbon_df[NB_SEATS_MIN] <= float(nb_seats)) & (
            carbon_df[NB_SEATS_MAX] >= float(nb_seats)
        )
        carbon_df = carbon_df[filter_nb_seats]
    logger.debug("Calculated CO2 for {} type over {} kms", type_transport, nb_km)
    return carbon_df["value"].mean() * nb_km
