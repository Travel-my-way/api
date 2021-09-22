# The stack

The whole stack is comprised of 4 main parts :

1. The [services](stack.md?id=services) (rabbitmq / redis / postgresql)
2. The [API](stack.md?id=api)
3. The [workers](stack.md?id=workers)
4. The [broker](stack.md?id=broker)

All of them must be started in order to correctly process the requests.

## Prerequisites

* Python 3.8+
* [pipenv](https://pipenv.pypa.io/en/latest/)
* [Docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/)
* Your favorite editor (PyCharm, Sublime Text, VSCode...)

## Installation

With all [prerequisites](stack.md?id=prerequisites) installed, simply run the following command to install all dependencies:

```bash
$ pipenv install --dev
```

Then proceed to start services, API, workers and broker.

## Services

All application components communicates using several services. All of these are provided over docker-compose for easier access and low-burden installation.

The mandatory services are theses ones:

* RabbitMQ for interprocess communication
* Redis for results storage
* PostgreSQL for metrics / persistent results.

Additionnal services are provided for development:

* [Flower](https://flower.readthedocs.io/en/latest/)

All of them can be started using docker-compose:

```bash
$ docker-compose up
```

## Common environments variables

All stack components, external ones excluded, share a set of common environments variables which **MUST** be provided to each component:

|           Name          |              Format              |              Meaning              |
|-------------------------|----------------------------------|-----------------------------------|
| `CELERY_BROKER_URL`     | `amqp://user:pass@broker:port//` | AMQP address of rabbitmq instance |
| `CELERY_RESULT_BACKEND` | `redis://user:pass@redis:port`   | Address of redis backend          |

!> The value of these variables **MUST** be the same everywhere for the application to be functional!

## API

The API is the component in charge of receiving queries from front app or any client and emit them to workers. After the computations are done, il will return the final journeys.

### Configuration

In addition to [common environment variables](stack.md?id=common-environments-variables), the API needs the following ones :

|     Name    |             Format            |                        Meaning                         |
|-------------|-------------------------------|--------------------------------------------------------|
| `WORKERS`   | `comma separated list of str` | The list of running workers on the system              |
| `FLASK_APP` | `api.lbv:create_app('<ENV>')` | If running in a non-production environment, like `dev` |

!> The `WORKERS` variable **MUST** exactly match the running workers. If not, broker will permanently wait for inexistant workers and no results will appear.

### Running

The broker can be started with the following commands:

```bash
# In pipenv shell
$ python -m flask run

# Outside
$ pipenv run flask

```

?> The API will now run on http://localhost:8000

How to change port or address can be found in [official documentation](https://flask.palletsprojects.com/en/2.0.x/cli/#run-the-development-server).

## Workers

The workers are the component in charge of communicating with external APIs like blablacar or kombo and provides theirs results to the broker.

### Configuration

In addition to [common environment variables](stack.md?id=common-environments-variables), the workers needs the following ones depending on each worker:

|      Worker     |           Name          | Format |         Meaning          |
|-----------------|-------------------------|--------|--------------------------|
| ouibus          | OUIBUS_API_KEY          | `str`  | OUIBUS  API key          |
| ors             | ORS_API_KEY             | `str`  | ORS  API key             |
| skyscanner      | SKYSCANNER_API_KEY      | `str`  | SKYSCANNER  API key      |
| skyscanner      | SKYSCANNER_RAPIDAPI_KEY | `str`  | SKYSCANNER_RAPID API key |
| kombo & ferries | KOMBO_API_KEY           | `str`  | KOMBO  API key           |
| blablacar       | BLABLACAR_API_KEY       | `str`  | BLABLACAR API key        |

?> All these environments variables are sensitive ones, you should be careful where you store them. We do use [Doppler](https://doppler.com/join?invite=ED2D7304) for our secrets management.

### Running

The broker can be started with the following commands:

```bash
# In pipenv shell
$ python -m flask run

# Outside
$ pipenv run flask

```

?> The API will now run on http://localhost:8000

How to change port or address can be found in [official documentation](https://flask.palletsprojects.com/en/2.0.x/cli/#run-the-development-server).

## Broker

The broker is the component in charge of receiving all journeys from workers, aggregate them, compute the best journey and provide it back to the API.

### Configuration

In addition to [common environment variables](stack.md?id=common-environments-variables), the broker needs the following ones :

|        Name       | Format |          Meaning           |
|-------------------|--------|----------------------------|
| `NAVITIA_API_KEY` | `str`  | API key of Navitia service |

### Running

The broker can be started with the following commands:

```bash
# In pipenv shell
$ python -m celery -A broker worker --hostname=broker@%h -l INFO

# Outside
$ pipenv run broker

```
