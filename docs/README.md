# BonVoyage, the back stack

Welcome to the documentation for the **back** stack of BonVoyage project.


BonVoyage is a low-carbon / carbon efficient travel search engine located at https://app.bonvoyage.eco/.

## Getting started

If you're new to the projects, you should read the [architecture and flow](architecture.md) page in order to understand how the magic happens. When you headache is gone, you can proceed to install [prerequisites](README.md?id=prerequisites) and, finally, the [stack](stack.md) itself.

## Prerequisites

* Python 3.8+
* [pipenv](https://pipenv.pypa.io/en/latest/)
* [Docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/)
* Your favorite editor (PyCharm, Sublime Text, VSCode...)


## Composants docker-compose

### PostgreSQL

Une base PG avec l'extension [PostGIS](https://postgis.net/) est déployée dans le docker-compose et aussi accessible en local sur le port 5432.

Les identifiants / mot de passe / ID de la base sont disponibles dans le fichier `docker-compose.yml`.

Cette base est aussi linkée dans les divers composants.


### Utilisation en développement de l'API

Plusieurs services sont déclarés dans docker-compose. Vous pouvez lancer [seulement certains containers](https://docs.docker.com/compose/reference/up/) et les utiliser
dans votre process de développement normal.

Les containers marqués comme **essentiels** dans le tableau ci-dessous doivent toujours être lancés.

#### Services déclarés

|     Nom      |                           Usage                           | Essentiel |
|--------------|-----------------------------------------------------------|-----------|
| api          | API en flask, consommée par le front                      | non       |
| rabbitmq     | Instance AMQP pour les appels de workers                  | oui       |
| redis        | Instance redis pour le stockage des résultats des worker  | oui       |
| redisinsight | Instance pour visualiser le contenu de l'instance redis   | non       |
| broker       | Service applicatif d'aggrégation des résultats des worker | non       |
| trainline    | Exemple de worker répondant sur le topic "train"          | non       |
| fake         | Exemple de worker répondant sur le topic "fake"           | non       |

La doc spécifique à chaque service du projet est dans le répertoire associé:

* [Doc de l'API](api/README.md)
* [Doc du broker](broker/README.md)
* [Doc des workers](worker/README.md)
