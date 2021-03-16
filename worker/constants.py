#PATHS
# ADEME_LOC_DB_PATH = r'data\TMW_base_carbone.xlsx'
ADEME_LOC_DB_PATH = r'data/EmissionFactor.csv'

# #SUBCATEGORIES
# SUBCAT = 'subcategory_2'
# SUBCAT_RAIL = 'Rail'
# SUBCAT_AIR = 'Air'
# SUBCAT_ROAD = 'Road'

#CARBON DB HEADER
# TYPE_OF_TRANSPORT = "Type of transport"
# LOCALISATION = "Localisation"
# NB_SEATS = "Number of seats"
# NB_KM = "Number of km"
# FUEL = "Fuel"
TYPE_OF_TRANSPORT = "subcategory_3"
CITY = 'city'
CITY_SIZE_MIN = 'city_size_min'
CITY_SIZE_MAX = 'city_size_max'
NB_SEATS_MIN = "capacity_min"
NB_SEATS_MAX = "capacity_max"
NB_KM_MIN = "distance_min"
NB_KM_MAX = "distance_max"
FUEL = "fuel"

# USAGES
TYPE_PLANE = "Plane"
TYPE_TRAIN = "Train"
TYPE_COACH = "Coach"   # Inter cities
TYPE_BUS = "Bus"       # Inner agglomeration
TYPE_METRO = "Metro"
TYPE_WAIT = "Waiting"
TYPE_AUTOMOBILE = "Automobile"
TYPE_METRO = "Metro"
TYPE_BIKE = "Bike"
TYPE_WALK = "Walking"
TYPE_TRANSFER = "Transfer"
TYPE_TRAM = "Tram"
TYPE_CAR = 'Car'
TYPE_CARPOOOLING = 'Carpooling'
TYPE_FERRY = 'Ferry'

# CITY SIZE
SMALL_CITY = ('', '0', '150000')  # "< 150 000 inhabitants"
MEDIUM_CITY = ('', '150000', '250000')  # "150 000 - 250 000 inhabitants"
BIG_CITY = ('', '250000', '1000000000')  # "> 250 000 inhabitants"
PARIS = ('Paris', '', '')  # "Paris"

#NB SEATS
NB_SEATS_TEST = ('180', '250')

# EMISSIONS UNITS
CO2EQ_PASS_KM = "kgCO2eq/passenger.km"

# UNITES
UNIT_CONVERSION = 1

# WAITING_PERIODS
WAITING_PERIOD_TRAINLINE = 15 * 60
WAITING_PERIOD_BLABLACAR = 15 * 60
WAITING_PERIOD_AIRPORT = 75 * 60
WAITING_PERIOD_OUIBUS = 15 * 60
WAITING_PERIOD_PORT = 15 * 60

# DEFAULT VALUES
DEFAULT_CITY = ('', '', '')
DEFAULT_NB_PASSENGERS = 1
DEFAULT_FUEL = ''
DEFAULT_NB_SEATS = ('', '')
DEFAULT_NB_KM = ('', '')

# Journey label
LABEL_CHEAPEST_JOURNEY = 'Cheapest Journey'
LABEL_FASTEST_JOURNEY = 'Fastest Journey'
LABEL_CLEANEST_JOURNEY = 'Cleanest Journey'

# Journey categories
CATEGORY_TRAIN_JOURNEY = 'Train'
CATEGORY_PLANE_JOURNEY = 'Plane'
CATEGORY_CAR_JOURNEY = 'Car'
CATEGORY_COACH_JOURNEY = 'Coach'
CATEGORY_CARPOOOLING_JOURNEY = 'Carpooling'
CATEGORY_FERRY_JOURNEY = 'Ferry'
