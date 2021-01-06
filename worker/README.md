# LBV: les workers

Les workers sont en charge de prendre une requête utilisateur et d'appeler leur api respectives pour récupérer les
résultats en rapport.

## Lancer en local

A la racine du repo, lancer l'installation des dépendances via pipenv.

```bash
$ pipenv install --dev
```

### Variables d'environnement

Un worker nécessite plusieurs variables d'environnements pour s'exécuter. Vous pouvez utiliser le fichier `env.tmpl` à la racine du repo & le copier sous le nom `.env` pour cela.

Il vous suffit ensuite de lancer la commande suivante :

```bash
$ pipenv run worker --name=<NOM_DU_WORKER>
```

Le nom du worker correspond au nom du module associé.
