import pandas as pd
from loguru import logger
from . import tmw_api_keys
from . import TMW as tmw
from . import constants
# import folium
from navitia_client import Client
from shapely.geometry import Point
import re
import unicodedata
from datetime import datetime
import shapely.wkt


def get_navitia_coverage(client):
    """
    The navitia API is separated into different coverage region (one for california, one for PAris-IDF ...)
    We call the API to get all those coverage and know which coverage region to call for a given scenario
    """
    # call API for all coverages
    response_cov = client.raw('coverage', multipage=False, page_limit=10, verbose=True)
    # turn coverage into DF
    df_cov = pd.DataFrame.from_dict(response_cov.json()['regions'])
    # extract the geographical shape into polygon
    df_cov['polygon_clean'] = df_cov.apply(clean_polygon_for_coverage, axis=1)
    # Clean NaN polygons
    df_cov = df_cov[~pd.isna(df_cov.polygon_clean)].reset_index(drop=True)
    # Get the area of each polygon
    df_cov['area_polygon_clean'] = df_cov.loc[:, 'polygon_clean'].apply(lambda x: x.area)
    return df_cov


def clean_polygon_for_coverage(x):
    # Test whether shape is not empty
    if x['shape'] == '':
        # Polygon is null
        return None
    # Transform the string in multipolygon
    mp_loc = shapely.wkt.loads(x['shape'])
    # Then into a list of polygon
    p_loc = list(mp_loc)
    # Test whether there are several polygons
    if len(p_loc) > 1:
        raise ValueError("ERROR: NAVITIA RETURNS MULTIPLE POLYGONS")
    # If not return the first (and only) one
    return p_loc[0]


def find_navita_coverage_for_points(point_from, point_to, df_cov):
    """
    This function finds in which coverage regions are the 2 points.
    If any point is not in any region, or the 2 points are in different regions we have an error
    """
    # convert into geopoint
    point_from = Point(point_from[1], point_from[0])
    point_to = Point(point_to[1], point_to[0])
    # test if points are within polygon for each region
    are_points_in_cov = df_cov.apply(
        lambda x: (x.polygon_clean.contains(point_from)) & (x.polygon_clean.contains(point_to)), axis=1)
    # extract the id of the smallest region around the point
    ix_id_cov = df_cov[are_points_in_cov].area_polygon_clean.idxmin()
    id_cov = df_cov.loc[ix_id_cov, 'id']
    return id_cov


"""
NAVITIA FUNCTIONS
"""


def start_navitia_client():
    navitia_api_key = tmw_api_keys.NAVITIA_API_KEY
    navitia_client = Client(user=navitia_api_key)
    return navitia_client

# ATTENTION: C'est quoi ce bout de code. Pas très propre... Doit être dans le main.py?
navitia_client = start_navitia_client()
_NAVITIA_COV = get_navitia_coverage(navitia_client)
logger.info(_NAVITIA_COV.head(1))


def navitia_query_directions(query, _id=0):
    navitia_client = start_navitia_client()
    try:
        navitia_region = find_navita_coverage_for_points(query.start_point, query.end_point, _NAVITIA_COV)
    except:
        logger.warning(f'on a pas trouve la region :( {query.start_point} {query.end_point}')
        return None
        # raise ValueError("ERROR: COVERAGE ISSUE")
    # if start.navitia['name'] != end.navitia['name']:  # region name (ex: idf-fr)
    #     print('ERROR: NAVITIA query on 2 different regions')

    # start_coord = ";".join(map(str, query.start_point))
    # end_coord = ";".join(map(str, query.end_point))
    start_coord = str(query.start_point[1]) + ";" + str(query.start_point[0])
    end_coord = str(query.end_point[1]) + ";" + str(query.end_point[0])
    url = f'coverage/{navitia_region}/journeys?from={start_coord}&to={end_coord}'
    url = url + '&data_freshness=base_schedule&max_nb_journeys=3'

    step = navitia_client.raw(url, multipage=False)
    if step.status_code == 200:
        return navitia_journeys(step.json())
        #return step.json()

    else:
        logger.warning(f'ERROR {step.status_code} from Navitia for {url}')
        return None


# FONCTION NON UTILISéE, A SUPPRIMER?
# def navitia_coverage_global():
#     navitia_client = start_navitia_client()
#     cov = navitia_client.raw('coverage', multipage=False, page_limit=10, verbose=True)
#     coverage = cov.json()
#     for i, region in enumerate(coverage['regions']):
#         coverage['regions'][i]['shape'] = navitia_geostr_to_polygon(region['shape'])
#     return coverage


# FONCTION NON UTILISéE, A SUPPRIMER?
# def navitia_coverage_plot(coverage):
#     _map = init_map(center=(48.864716, 2.349014), zoom_start=4)
#     for zone in coverage['regions']:
#         folium.vector_layers.PolyLine(locations=zone['shape'],  # start converage
#                                       tooltip=zone['name'],
#                                       smooth_factor=1,
#                                       ).add_to(_map)
#     return _map


#UTILISER DANS LA CLASSE POINT DE TMW, MAIS CETTE CLASSE EST UTILISéE NUL PART, A SUPPRIMER?
def navitia_coverage_gpspoint(lon, lat):  #
    navitia_client = start_navitia_client()
    cov = navitia_client.raw('coverage/{};{}'.format(lon, lat), multipage=False, page_limit=10, verbose=True)
    coverage = cov.json()
    try:
        for i, region in enumerate(coverage['regions']):
            coverage['regions'][i]['shape'] = navitia_geostr_to_polygon(region['shape'])
    except:
        logger.error('ERROR: AREA NOT COVERED BY NAVITIA (lon:{},lat:{})'.format(lon, lat))
        return False
    return coverage


def navitia_geostr_to_polygon(string):
    regex = r"([-]?\d+\.\d+) ([-]?\d+\.\d+)"
    r = re.findall(regex, string)
    r = [(float(coord[1]), float(coord[0])) for coord in r]  # [ (lat, lon) , (), ()]
    return r


# FONCTION NON UTILISéE, A SUPPRIMER?
# def point_in_polygon(point, polygon):
#     import shapely
#     from shapely.geometry import Polygon
#     poly = Polygon(((p[0],p[1])for p in polygon))
#     return True


"""
https://doc.navitia.io/#journeys
type = 'waiting' / 'transfer' / 'public_transport' / 'street_network' / 'stay_in' / crow_fly
"""


def navitia_journeys(json, _id=0):
    # all journeys loop
    lst_journeys = list()
    try:
        journeys = json['journeys']
    except:
        logger.warning('ERROR {}'.format(json['error']))
        return None
    for j in journeys:
        i = _id
        # journey loop
        lst_sections = list()
        for section in j['sections']:
            try:
                lst_sections.append(navitia_journeys_sections_type(section, _id=i))
            except:
                logger.warning('Navitia ERROR : ')
                logger.warning('id: {}'.format(i))
                logger.warning(section)
            i = i + 1
        lst_journeys.append(tmw.Journey(_id, steps=lst_sections))
    return lst_journeys


def navitia_journeys_sections_type(json, _id=0):
    switcher_journeys_sections_type = {
        'public_transport': navitia_journeys_sections_type_public_transport,
        'street_network': navitia_journeys_sections_type_street_network,
        'waiting': navitia_journeys_sections_type_waiting,
        'transfer': navitia_journeys_sections_type_transfer,
        'on_demand_transport': navitia_journeys_sections_type_on_demand,
    }
    func = switcher_journeys_sections_type.get(json['type'], "Invalid navitia type")
    step = func(json, _id)
    return step


def navitia_journeys_sections_type_on_demand(json, _id=0):
    display_information = json['display_informations']
    label = '{} {} / {} / direction: {}'.format(
        display_information['physical_mode'],
        display_information['code'],
        display_information['name'],
        display_information['direction'],
    )
    embedded_type_from = json['from']['embedded_type']
    embedded_type_to = json['to']['embedded_type']

    departure_point = [float(json['from'][embedded_type_from]['coord']['lat']),
                       float(json['from'][embedded_type_from]['coord']['lon'])]
    arrival_point = [float(json['to'][embedded_type_to]['coord']['lat']),
                     float(json['to'][embedded_type_to]['coord']['lon'])]
    step = tmw.Journey_step(_id,
                            _type=display_information['network'].lower(),
                            label=label,
                            distance_m=json['geojson']['properties'][0]['length'],
                            duration_s=json['duration'],
                            price_EUR=[0],
                            gCO2=json['co2_emission']['value'],
                            departure_point=departure_point,
                            arrival_point=arrival_point,
                            departure_stop_name=json['from']['name'],
                            arrival_stop_name=json['to']['name'],
                            departure_date=datetime.strptime(json['departure_date_time'], '%Y%m%dT%H%M%S').timestamp(),
                            arrival_date=datetime.strptime(json['arrival_date_time'], '%Y%m%dT%H%M%S').timestamp(),
                            geojson=json['geojson'],
                            )
    return step


def navitia_journeys_sections_type_public_transport(json, _id=0):
    display_information = json['display_informations']
    label = '{} {} / {} / direction: {}'.format(
        display_information['physical_mode'],
        display_information['code'],
        display_information['name'],
        display_information['direction'],
    )
    switcher_public_transport_type = {
        'Métro': constants.TYPE_METRO,
        'Metro': constants.TYPE_METRO,
        'Bus': constants.TYPE_BUS,
        'Tramway': constants.TYPE_TRAM,
        'RER': constants.TYPE_METRO,
        'Train': constants.TYPE_METRO,
    }
    _type = switcher_public_transport_type.get(display_information['commercial_mode'],
                                               "unknown public transport")
    # _type = display_information['commercial_mode']
    # _type = unicodedata.normalize('NFD', _type).encode('ascii', 'ignore').lower()

    embedded_type_from = json['from']['embedded_type']
    embedded_type_to = json['to']['embedded_type']

    departure_point = [float(json['from'][embedded_type_from]['coord']['lat']),
                       float(json['from'][embedded_type_from]['coord']['lon'])]
    arrival_point = [float(json['to'][embedded_type_to]['coord']['lat']),
                     float(json['to'][embedded_type_to]['coord']['lon'])]

    step = tmw.Journey_step(_id,
                            _type=_type,
                            label=label,
                            distance_m=json['geojson']['properties'][0]['length'],
                            duration_s=json['duration'],
                            price_EUR=[0],
                            gCO2=json['co2_emission']['value'],
                            departure_point=departure_point,
                            arrival_point=arrival_point,
                            departure_stop_name=json['from']['name'],
                            arrival_stop_name=json['to']['name'],
                            departure_date=datetime.strptime(json['departure_date_time'], '%Y%m%dT%H%M%S').timestamp(),
                            arrival_date=datetime.strptime(json['arrival_date_time'], '%Y%m%dT%H%M%S').timestamp(),
                            geojson=json['geojson'],
                            )

    return step


def navitia_journeys_sections_type_street_network(json, _id=0):
    mode = json['mode']
    mode_to_type = {
        'walking': constants.TYPE_WALK,
        'bike': constants.TYPE_BIKE,
        'car': constants.TYPE_CAR,
    }
    label = '{} FROM {} TO {}'.format(
        mode_to_type[mode],
        json['from']['name'],
        json['to']['name'],
    )
    embedded_type_from = json['from']['embedded_type']
    embedded_type_to = json['to']['embedded_type']

    departure_point = [float(json['from'][embedded_type_from]['coord']['lat']),
                       float(json['from'][embedded_type_from]['coord']['lon'])]
    arrival_point = [float(json['to'][embedded_type_to]['coord']['lat']),
                     float(json['to'][embedded_type_to]['coord']['lon'])]
    step = tmw.Journey_step(_id,
                            _type=mode_to_type[mode],
                            label=label,
                            distance_m=json['geojson']['properties'][0]['length'],
                            duration_s=json['duration'],
                            price_EUR=[0],
                            gCO2=json['co2_emission']['value'],
                            departure_point=departure_point,
                            arrival_point=arrival_point,
                            departure_stop_name=json['from']['name'],
                            arrival_stop_name=json['to']['name'],
                            departure_date=datetime.strptime(json['departure_date_time'], '%Y%m%dT%H%M%S').timestamp(),
                            arrival_date=datetime.strptime(json['arrival_date_time'], '%Y%m%dT%H%M%S').timestamp(),
                            geojson=json['geojson'],
                            )
    return step


def navitia_journeys_sections_type_transfer(json, _id=0):
    mode = json['transfer_type']
    mode_to_type = {
        'walking': constants.TYPE_WALK,
        'bike': constants.TYPE_BIKE,
        'car': constants.TYPE_CAR,
    }
    label = '{} FROM {} TO {}'.format(mode_to_type[mode], json['from']['name'], json['to']['name'])
    embedded_type_from = json['from']['embedded_type']
    embedded_type_to = json['to']['embedded_type']

    departure_point = [float(json['from'][embedded_type_from]['coord']['lat']),
                       float(json['from'][embedded_type_from]['coord']['lon'])]
    arrival_point = [float(json['to'][embedded_type_to]['coord']['lat']),
                     float(json['to'][embedded_type_to]['coord']['lon'])]
    step = tmw.Journey_step(_id,
                            _type=mode_to_type[mode],
                            label=label,
                            distance_m=json['geojson']['properties'][0]['length'],
                            duration_s=json['duration'],
                            price_EUR=[0],
                            gCO2=json['co2_emission']['value'],
                            departure_point=departure_point,
                            arrival_point=arrival_point,
                            departure_stop_name=json['from']['name'],
                            arrival_stop_name=json['to']['name'],
                            departure_date=datetime.strptime(json['departure_date_time'], '%Y%m%dT%H%M%S').timestamp(),
                            arrival_date=datetime.strptime(json['arrival_date_time'], '%Y%m%dT%H%M%S').timestamp(),
                            geojson=json['geojson'],
                            )
    return step


def navitia_journeys_sections_type_waiting(json, _id=0):
    step = tmw.Journey_step(_id,
                            _type=constants.TYPE_WAIT,
                            label='wait',
                            distance_m=0,
                            duration_s=json['duration'],
                            price_EUR=[0],
                            gCO2=0,
                            departure_point=[0,0],
                            arrival_point=[0,0],
                            departure_stop_name='',
                            arrival_stop_name='',
                            departure_date=datetime.strptime(json['departure_date_time'], '%Y%m%dT%H%M%S').timestamp(),
                            arrival_date=datetime.strptime(json['arrival_date_time'], '%Y%m%dT%H%M%S').timestamp(),
                            geojson='',
                            )
    return step


# FONCTION NON UTILISéE, A SUPPRIMER?
# def navitia_journeys_correct(journey, json):
#     try:
#         if type(j) == journey:
#             True
#     except:
#         print('ERROR function navitia_journeys_correct() - INPUT Not journey class')
#         return False
#
#     return journey

# ATTENTION NOTION DE TIMEZONE A PRENDRE EN COMPTE NORMALEMENT, A DISCUTER
# import pytz
# TZ_LOC = pytz.timezone('Europe/Paris')
# TZ_LOC.localize(...)

