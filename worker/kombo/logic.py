import requests
import pandas as pd
from datetime import timedelta, datetime as dt
import copy
import time
from loguru import logger
from geopy.distance import distance
from worker.carbon import emission

from .. import config as tmw_api_keys, constants
from .. import TMW


def init():
    logger.info("Global init")
    emission.init_carbon()

    return {"city_db": update_city_list()}


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
    for row in df.to_dict(orient="records"):

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


def count_relevant_stations(city):
    relevant_stations = filter(lambda station: len(station['transportType']) > 0, city['stations'])

    return len(list(relevant_stations))


def update_city_list():
    headers = {
        "token": tmw_api_keys.KOMBO_API_KEY,
    }
    r = requests.get("https://turing.kombo.co/city", headers=headers)
    if r.status_code == 200:
        cities = pd.DataFrame.from_dict(r.json())
        cities["nb_stations"] = cities.apply(count_relevant_stations, axis=1)
        cities = cities[cities["nb_stations"] > 0]
        cities.to_csv('city_kombo.csv', index=False)
        return cities

    else:
        return None


# Find the stops close to a geo point
def get_cities_from_geo_locs(geoloc_dep, geoloc_arrival, city_db, nb_different_city=2):
    """
    This function takes in the departure and arrival points of the TMW journey and returns
        the 3 closest stations within 50 km
    """
    # We filter only on the closest stops to have a smaller df (thus better perfs)
    stops_tmp = city_db[
        (
            ((city_db.latitude - geoloc_dep[0]) ** 2 < 0.6)
            & ((city_db.longitude - geoloc_dep[1]) ** 2 < 0.6)
        )
        | (
            ((city_db.latitude - geoloc_arrival[0]) ** 2 < 0.6)
            & ((city_db.longitude - geoloc_arrival[1]) ** 2 < 0.6)
        )
    ].copy()

    stops_tmp["distance_dep"] = stops_tmp.apply(
        lambda x: (geoloc_dep[0] - x.latitude) ** 2
        + (geoloc_dep[1] - x.longitude) ** 2,
        axis=1,
    )
    stops_tmp["distance_arrival"] = stops_tmp.apply(
        lambda x: (geoloc_arrival[0] - x.latitude) ** 2
        + (geoloc_arrival[1] - x.longitude) ** 2,
        axis=1,
    )

    # We get all station within approx 55 km (<=> 0.5 of distance proxi)
    parent_station_id_list = dict()

    # We keep only the 2 closest cities from dep and arrival

    parent_station_id_list["origin"] = (
        stops_tmp[stops_tmp.distance_dep < 0.5]
        .sort_values(by="nb_stations", ascending=False)
        .head(nb_different_city)
        .id
    )
    parent_station_id_list["arrival"] = (
        stops_tmp[stops_tmp.distance_arrival < 0.5]
        .sort_values(by="nb_stations", ascending=False)
        .head(nb_different_city)
        .id
    )

    return parent_station_id_list


def enrich_trips(trips, stations, companies):
    trips_exploded = pandas_explode(pandas_explode(trips, "segments"), "segments")
    trips_grouped = trips_exploded.groupby("tripId").agg({"segments": "count"})
    trips_exploded[
        [
            "departureStationId",
            "arrivalStationId",
            "departureTime",
            "arrivalTime",
            "companyId",
        ]
    ] = trips_exploded.apply(extract_from_segments, axis=1)
    trips_exploded = trips_exploded.merge(
        stations, left_on="departureStationId", right_on="id", suffixes=["", "_dep"]
    )
    trips_exploded = trips_exploded.merge(
        stations, left_on="arrivalStationId", right_on="id", suffixes=["", "_arr"]
    )
    trips_exploded = trips_exploded.merge(
        companies, left_on="companyId", right_on="id", suffixes=["", "_comp"]
    )
    trips_exploded = trips_exploded.merge(
        trips_grouped, on="tripId", suffixes=["", "_nb"]
    )
    trips_exploded["departureTime"] = pd.to_datetime(trips_exploded["departureTime"])
    trips_exploded["arrivalTime"] = pd.to_datetime(trips_exploded["arrivalTime"])
    # we get rid of segments so we can deduplicate later
    trips_exploded.pop("segments")

    return trips_exploded


def dataframize(json_object):
    tmp_list = list()
    for key in json_object:
        tmp_list.append(json_object[key])
    return pd.DataFrame(tmp_list)


def extract_from_segments(x):
    return pd.Series(
        [
            x["segments"]["departureStationId"],
            x["segments"]["arrivalStationId"],
            x["segments"]["departureTime"],
            x["segments"]["arrivalTime"],
            x["segments"]["companyId"],
        ]
    )


def extract_from_stations(x):
    return pd.Series(
        [
            x["id"]["name"],
            x["latitude"]["longitude"],
            x["cityId"]["departureTime"],
            x["segments"]["arrivalTime"],
            x["segments"]["companyId"],
        ]
    )


def response_ready(fast_response, trips, companies):
    if fast_response & (len(trips) > 0) & (len(companies) > 0):
        try:
            if (
                "bus" in companies.transportType.unique()
            ) or "train" in companies.transportType.unique():
                return True
        except Exception as e:
            logger.warning(f"fast_response bug {e}")
            return False
    return False


def search_kombo(id_dep, id_arr, date, nb_passengers=1, fast_response=False):
    headers = {
        "token": tmw_api_keys.KOMBO_API_KEY,
    }
    r = requests.get(
        f"https://turing.kombo.co/search/{id_dep}/{id_arr}/{date}/{nb_passengers}",
        headers=headers,
    )
    logger.info(
        f"https://turing.kombo.co/search/{id_dep}/{id_arr}/{date}/{nb_passengers}"
    )
    if r.ok:
        pollkey = r.json()["key"]
    else:
        logger.warning(f"error in search {r.status_code}")
        return pd.DataFrame()
    time.sleep(0.1)
    try:
        logger.info("Polling kombo results...")
        response = requests.get(
            f"https://turing.kombo.co/pollSearch/{pollkey}", headers=headers
        )
        trips = pd.DataFrame.from_dict(response.json()["trips"])
        stations = dataframize(response.json()["dependencies"]["stations"])
        companies = dataframize(response.json()["dependencies"]["companies"])
        keep_looking = not (response.json()["completed"])
    except Exception as e:
        logger.warning("error in kombo response: {}", e)
        return pd.DataFrame()

    if response_ready(fast_response, trips, companies):
        keep_looking = False

    while keep_looking:
        time.sleep(0.5)
        response = requests.get(
            f"https://turing.kombo.co/pollSearch/{pollkey}", headers=headers
        )
        trips = trips.append(pd.DataFrame.from_dict(response.json()["trips"]))
        stations = stations.append(
            dataframize(response.json()["dependencies"]["stations"])
        )
        companies = companies.append(
            dataframize(response.json()["dependencies"]["companies"])
        )
        keep_looking = not (response.json()["completed"])

    if len(trips) == 0:
        logger.info("trips est vide")
        return pd.DataFrame()
    logger.info("Got {} trips from kombo", len(trips))
    stations = stations.drop_duplicates()
    companies = companies.drop_duplicates()
    trips_clean = enrich_trips(trips, stations, companies)
    return trips_clean


def filter_journeys(df):
    limit_train = 5
    limit_plane = 2
    limit_coach = 5

    train_trips = list(df[df.transportType == 'train'].tripId)
    coach_trips = list(df[df.transportType == 'bus'].tripId)
    plane_trips = list(df[df.transportType == 'flight'].tripId)

    train_trips = [trip for trip in train_trips if trip not in plane_trips]
    train_trips = train_trips[0: min(len(train_trips), limit_train)]

    coach_trips = [trip for trip in coach_trips if trip not in plane_trips]
    coach_trips = coach_trips[0: min(len(coach_trips), limit_coach)]

    plane_trips = plane_trips[0: min(len(plane_trips), limit_plane)]

    logger.info(plane_trips)
    df_filtered = df[df.tripId.isin(train_trips + coach_trips + plane_trips)]

    return df_filtered


def kombo_journey(df_response, passengers=1):
    """
    This function takes in a DF with detailled info about all the Kombo trips
    It returns a list of TMW journey objects
    """
    df_response = filter_journeys(df_response)
    # affect a price to each leg (otherwise we would multiply the price by the number of legs
    df_response["price_step"] = df_response['price'] / (df_response['segments_nb'] * 100)

    # Compute distance for each leg
    # print(df_response.columns)
    df_response["distance_step"] = df_response.apply(
        lambda x: distance(
            [x.latitude, x.longitude], [x.latitude_arr, x.longitude_arr]
        ).m,
        axis=1,
    )

    lst_journeys = list()
    tranportation_mean_wait = {
        "bus": constants.WAITING_PERIOD_OUIBUS,
        "train": constants.WAITING_PERIOD_TRAINLINE,
        "flight": constants.WAITING_PERIOD_AIRPORT,
    }
    switcher_category = {
        "train": constants.TYPE_TRAIN,
        "flight": constants.TYPE_PLANE,
        "bus": constants.TYPE_COACH,
    }

    # all itineraries :
    df_response["latitude"] = df_response.apply(lambda x: float(x["latitude"]), axis=1)
    df_response["longitude"] = df_response.apply(
        lambda x: float(x["longitude"]), axis=1
    )
    df_response["latitude_arr"] = df_response.apply(
        lambda x: float(x["latitude_arr"]), axis=1
    )
    df_response["longitude_arr"] = df_response.apply(
        lambda x: float(x["longitude_arr"]), axis=1
    )

    # print(f'nb itinerary : {df_response.id_global.nunique()}')
    id_journey = 0

    for tripId in df_response.tripId.unique():
        itinerary = df_response[df_response.tripId == tripId].reset_index(drop=True)
        # Make sure the legs of the trip are in the right order
        itinerary = itinerary.sort_values(by="departureTime")
        # boolean to know whether and when there will be a transfer after the leg
        itinerary["next_departure"] = itinerary.departureTime.shift(-1)
        itinerary["next_stop_name"] = itinerary.name.shift(-1)
        itinerary["next_latitude"] = itinerary.latitude.shift(-1)
        itinerary["next_longitude"] = itinerary.longitude.shift(-1)

        i = 0
        lst_sections = list()

        station_wait = tranportation_mean_wait[itinerary.transportType.iloc[0]]
        # We add a waiting period at the station of 15 minutes
        step = TMW.Journey_step(
            i,
            _type=constants.TYPE_WAIT,
            label=f"Arrive at the station {station_wait}s before departure",
            distance_m=0,
            duration_s=station_wait,
            price_EUR=[0],
            gCO2=0,
            departure_point=[itinerary.latitude.iloc[0], itinerary.longitude.iloc[0]],
            arrival_point=[itinerary.latitude.iloc[0], itinerary.longitude.iloc[0]],
            departure_date=int(
                (
                    itinerary.departureTime[0] - timedelta(seconds=station_wait)
                ).timestamp()
            ),
            arrival_date=int(itinerary.departureTime[0].timestamp()),
            departure_stop_name=itinerary.name.iloc[0],
            arrival_stop_name=itinerary.name.iloc[0],
            bike_friendly=False,
            geojson=[],
        )

        lst_sections.append(step)
        i = i + 1
        # Go through all steps of the journey
        for index, leg in itinerary.iterrows():
            # logger.info("Journey step from {} to {}", leg["name"], leg.name_arr)
            local_distance_m = distance(
                [leg.latitude, leg.longitude], [leg.latitude_arr, leg.longitude_arr]
            ).m
            local_transportation_type = switcher_category[leg.transportType]
            local_emissions = emission.calculate_co2_emissions(
                local_transportation_type, local_distance_m
            )
            station_from = leg["name"]
            step = TMW.Journey_step(
                i,
                _type=local_transportation_type,
                label=f"{local_transportation_type} from {station_from} to {leg.name_arr}",
                distance_m=int(local_distance_m),
                duration_s=(leg.arrivalTime - leg.departureTime).seconds,
                price_EUR=[leg.price_step],
                gCO2=int(local_emissions),
                departure_point=[leg.latitude, leg.longitude],
                arrival_point=[leg.latitude_arr, leg.longitude_arr],
                departure_stop_name=leg['name'],
                arrival_stop_name=leg.name_arr,
                departure_date=int(leg.departureTime.timestamp()),
                arrival_date=int(leg.arrivalTime.timestamp()),
                trip_code="",
                bike_friendly=False,
                geojson=[],
                booking_link=f"www.kombo.co/affilate/1/fr/{passengers}/{leg.tripId}",
            )
            lst_sections.append(step)
            i = i + 1
            # add transfer steps
            if not pd.isna(leg.next_departure):
                # logger.info("Calculating next step")
                step = TMW.Journey_step(
                    i,
                    _type=constants.TYPE_TRANSFER,
                    label=f"Transfer at {leg.name_arr}",
                    distance_m=0,
                    duration_s=int(
                        (leg["next_departure"] - leg["arrivalTime"]).seconds
                    ),
                    price_EUR=[0],
                    departure_point=[leg.latitude_arr, leg.longitude_arr],
                    arrival_point=[leg.next_latitude, leg.next_longitude],
                    departure_stop_name=leg.name_arr,
                    arrival_stop_name=leg.next_stop_name,
                    departure_date=int(leg.arrivalTime.timestamp()),
                    arrival_date=int(leg.next_departure.timestamp()),
                    gCO2=0,
                    bike_friendly=True,
                    geojson=[],
                )
                lst_sections.append(step)
                i = i + 1

        journey_train = TMW.Journey(
            id_journey,
            steps=lst_sections,
            departure_date=lst_sections[0].departure_date,
            arrival_date=lst_sections[-1].arrival_date,
            # booking_link=f"www.kombo.co/affilate/1/fr/{passengers}/{leg.tripId}",
        )
        journey_train.update()
        # Add category
        category_journey = list()
        for step in journey_train.steps:
            if step.type not in [constants.TYPE_TRANSFER, constants.TYPE_WAIT]:
                category_journey.append(step.type)

        journey_train.category = list(set(category_journey))
        lst_journeys.append(journey_train)
        id_journey += 1

    return lst_journeys


def compute_kombo_journey(all_cities, start, fast_response=False):
    logger.info("Computing journeys")
    all_trips = pd.DataFrame()

    start = dt.fromtimestamp(start)
    found_low_carbon = False
    for origin_city in all_cities["origin"]:
        for arrival_city in all_cities["arrival"]:
            all_trips = all_trips.append(
                search_kombo(
                    origin_city,
                    arrival_city,
                    start.strftime("%Y-%m-%d"),
                    nb_passengers=1,
                    fast_response=fast_response,
                )
            )
            # Stop looking when we found train journeys
            if len(all_trips) > 0:
                if len(all_trips[all_trips["transportType"].isin(["train", "bus"])]) > 0:
                    found_low_carbon = True
                    break
            time.sleep(0.1)
        if found_low_carbon:
            break

    if len(all_trips) == 0:
        logger.warning("pas de all_trips")
        return list()

    all_trips = all_trips.drop_duplicates()

    return kombo_journey(all_trips)