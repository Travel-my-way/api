# Bon voyage, l'API

Ce repo contient le code pour faire fonctionner l'API BonVoyage en local (docker)

## Prérequis

* Docker

Si vous développez sur l'API, vous devez aussi avoir installé [pipenv](https://pipenv.pypa.io/en/latest/) et avoir python 3.8 disponible ([pyenv](https://github.com/pyenv/pyenv) vous aidera sur ce coup)

## Composants docker-compose

### PostgreSQL

Une base PG avec l'extension [PostGIS](https://postgis.net/) est déployée dans le docker-compose et aussi accessible en local sur le port 5432.

Les identifiants / mot de passe / ID de la base sont disponibles dans le fichier `docker-compose.yml`.

Cette base est aussi linkée dans les divers composants.

## Lancer l'API

### Utilisation uniquement (cas du front)
)
Si vous voulez uniquement lancer l'API pour la consommer par le front, il vous suffit de lancer la commande suivante:

```bash
make front
```

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
