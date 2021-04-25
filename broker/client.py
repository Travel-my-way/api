import json
from kombu import Exchange, Connection
from kombu import Queue
from kombu.mixins import ConsumerMixin
from loguru import logger
from redis import Redis

from . import Navitia
from . import TMW


def recreate_journey_objects(results_list):
    # # logger.info('into recreate_journey_objects')
    # # logger.info(results_list)
    journey_list = list()
    for result in results_list:
        # logger.info(result)
        journey_list.append(json_to_journey(result))

    return journey_list


def json_to_journey(json_journey):
    step_list = list()
    # logger.info('into json_to_journey')
    # logger.info(type(json_journey))
    # logger.info(json_journey)
    for step in json_journey['journey_steps']:
        step_list.append(TMW.Journey_step(0,
                                          _type=step['type'],
                                          label=step['label'],
                                          distance_m=float(step['distance_m']),
                                          duration_s=float(step['duration_s']),
                                          price_EUR=step['price_EUR'],
                                          departure_point=step['departure_point'],
                                          arrival_point=step['arrival_point'],
                                          departure_stop_name=step['departure_stop_name'],
                                          arrival_stop_name=step['arrival_stop_name'],
                                          departure_date=int(step['departure_date']),
                                          arrival_date=int(step['arrival_date']),
                                          gCO2=float(step['gCO2']),
                                          # bike_friendly=step['bike_friendly'],
                                          )
                         )
    journey = TMW.Journey(0,
                          steps=step_list,
                          departure_date=int(json_journey['departure_date']),
                          arrival_date=int(json_journey['arrival_date']),
                          booking_link=json_journey['booking_link'])

    journey.category = json_journey['category']
    return journey


class Client(ConsumerMixin):
    def __init__(self, connection: Connection, redis_url: str) -> None:
        self.connection = connection

        self.redis = Redis.from_url(redis_url)

    def get_consumers(self, Consumer, channel) -> list:
        return [
            Consumer(
                [Queue("results", Exchange("results"), routing_key="results")],
                callbacks=[self.on_message],
                accept=["json"],
            ),
        ]

    def on_message(self, body, message) -> None:
        correlation_id = message.properties["correlation_id"]
        with logger.contextualize(corrid=correlation_id):
            set_name = "request_id:{} type:partial_results".format(correlation_id)

            logger.info("Adding message to collation set...")
            try:
                self.redis.sadd(set_name, json.dumps(body))
            except Exception as e:
                logger.error("An error occured during Redis insert: {}", e)
            finally:
                message.ack()  # Always ack
                results = self.compute_results(request_id=correlation_id)
                self.store_results(request_id=correlation_id, results=results)

    def store_results(self, request_id: str, results: dict) -> None:
        self.redis.set(
            "request_id:{} type:final_results".format(request_id), json.dumps(results)
        )

    def compute_results(self, request_id: str) -> dict:
        logger.info("Computing result for set {}", request_id)

        # Load all partial results
        set = self.redis.smembers(
            "request_id:{} type:partial_results".format(request_id)
        )

        # MAGIC happens here !
        ## The following is NOT magic :)
        partials = [json.loads(_) for _ in set]

        content = {}

        journey_list = list()
        params = self.redis.get(
                "request_id:{} type:params".format(request_id)
        )
        params_json = json.loads(params)

        geoloc_dep = params_json['from'].split(',')
        geoloc_dep[0] = float(geoloc_dep[0])
        geoloc_dep[1] = float(geoloc_dep[1])
        geoloc_arr = params_json['to'].split(',')
        geoloc_arr[0] = float(geoloc_arr[0])
        geoloc_arr[1] = float(geoloc_arr[1])

        for o in partials:
            try:
                journey_list = journey_list + recreate_journey_objects(o["result"])
            except Exception as e:
                logger.warning(f'recreate_journey_objects a foiré pour {o["emitter"]}')
                logger.warning(e)
            # all_journeys = recreate_journey_objects(o["results"])
            tmp = len(o["result"])
            tmp2 = o["emitter"]
            logger.info(f'on a {tmp} journey de {tmp2}')
            content[o["emitter"]] = o["result"]

        urban_queries = list()
        logger.info(f'on a {len(journey_list)} journey')
        for interurban_journey in journey_list:
            if len(interurban_journey.steps[0].departure_point) == 1:
                logger.warning('mauvais trip')
                pass
            urban_queries.append(TMW.Query(0, geoloc_dep,
                                           interurban_journey.steps[0].departure_point,
                                           params_json['start']))
            urban_queries.append(TMW.Query(0, interurban_journey.steps[-1].arrival_point,
                                           geoloc_arr, interurban_journey.steps[-1].arrival_date))


        # Deduplicate queries
        # urban_queries = list(set(urban_queries))

        urban_journey_dict = dict()
        for urban_query in urban_queries:
            urban_journey = Navitia.navitia_query_directions(urban_query)

            urban_journey_dict[str(urban_query.to_json())] = urban_journey

        for interurban_journey in journey_list:
            json_key_start = TMW.Query(0, geoloc_dep,
                                       interurban_journey.steps[0].departure_point,
                                       params_json['start']).to_json()
            start_to_station_steps = urban_journey_dict[str(json_key_start)]
            json_key_end = TMW.Query(0, interurban_journey.steps[-1].arrival_point,
                                     geoloc_arr, interurban_journey.steps[-1].arrival_date).to_json()
            station_to_arrival_steps = urban_journey_dict[str(json_key_end)]

            if (start_to_station_steps is not None) & (station_to_arrival_steps is not None):
                if (start_to_station_steps[0] is not None) & (station_to_arrival_steps[0] is not None):
                    interurban_journey.add_steps(start_to_station_steps[0].steps, start_end=True)
                    interurban_journey.add_steps(station_to_arrival_steps[0].steps, start_end=False)

            interurban_journey.update()

        response = dict()
        i = 0
        for journey in journey_list:
            # On peut avoir plusieurs category pour le meme journey
            response[' + '.join(journey.category) + '_' + str(i)] = journey.to_json()
            i = i + 1

        return response
