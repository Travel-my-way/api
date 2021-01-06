# LBV: le broker

Le broker est en charge de récupérer les résultats renvoyés par les différents worker, les stocker dans redis puis de les utiliser pour construire des trajets selon la requête de l'utilisateur et de stocker ces trajets dans redis pour consommation par l'API

## Lancer en local

A la racine du repo, lancer l'installation des dépendances via pipenv.

```bash
$ pipenv install --dev
```

### Variables d'environnement

Le broker nécessite plusieurs variables d'environnements pour s'exécuter. Vous pouvez utiliser le fichier `env.tmpl` à la racine du repo & le copier sous le nom `.env` pour cela.

Il vous suffit ensuite de lancer la commande suivante :

```bash
$ pipenv run broker
```
