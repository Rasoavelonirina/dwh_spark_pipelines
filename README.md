# Projet de Pipelines de Données Spark (data_pipelines_project)

Ce projet contient un framework et des jobs PySpark conçus pour exécuter divers pipelines de traitement de données (ETL). Il vise la réutilisabilité, la configurabilité et la maintenabilité.

## Table des Matières

1.  [Architecture du Projet](#architecture-du-projet)
2.  [Prérequis](#prérequis)
3.  [Configuration](#configuration)
    *   [Configuration Commune](#configuration-commune)
    *   [Configuration Spécifique aux Jobs](#configuration-spécifique-aux-jobs)
4.  [Exécution des Jobs](#exécution-des-jobs)
    *   [Utilisation du Script `run_spark_job.sh`](#utilisation-du-script-run_spark_jobsh)
    *   [Exemples de Commandes](#exemples-de-commandes)
5.  [Jobs Implémentés](#jobs-implémentés)
    *   [zebra_last_transaction](#zebra_last_transaction)
6.  [Ajouter un Nouveau Job](#ajouter-un-nouveau-job)
    *   [Structure du Répertoire](#structure-du-répertoire)
    *   [Fichier de Configuration](#fichier-de-configuration)
    *   [Code du Job (`main.py`, `loader.py`, `transformer.py`, `writer.py`)](#code-du-job)
    *   [Utilisation des Modules Communs](#utilisation-des-modules-communs)
    *   [Enregistrement (Optionnel)](#enregistrement-optionnel)
    *   [Exécution du Nouveau Job](#exécution-du-nouveau-job)
7.  [Tests](#tests)
8.  [Considérations de Sécurité](#considérations-de-sécurité)

---

## 1. Architecture du Projet

Le projet suit une structure modulaire pour séparer la configuration, le code commun et la logique spécifique à chaque job :

```
data_pipelines_project/
├── config/                     # Fichiers de configuration
│   ├── common/                 # Configs communes (BDD, SFTP, Spark defaults)
│   │   ├── db_connections.ini
│   │   ├── remote_access.ini
│   │   └── spark_defaults.yaml
│   ├── jobs/                   # Configs spécifiques par job
│   │   └── zebra_last_transaction.yaml
│   │   └── ... (autres jobs)
│   └── config.yaml.template    # Modèle pour la config des jobs
├── docs/                        # NOUVEAU: Dossier pour la documentation
│   ├── jobs/                    # Documentation spécifique aux jobs
│   │   ├── zebra_last_transaction.md # README pour ce job
│   │   └── customer_segmentation.md  # README pour un autre job
│   │   └── ...
│   ├── architecture.md          # (Optionnel) Diagrammes d'architecture globale
│   └── common_modules.md        # (Optionnel) Doc détaillée des modules communs
│
├── src/                        # Code source Python
│   ├── common/                 # Modules utilitaires partagés
│   │   ├── __init__.py
│   │   ├── base_data_handler.py # Interface pour les handlers
│   │   ├── config.py           # Chargement/Fusion de config
│   │   ├── data_clients.py     # Factory pour obtenir les handlers
│   │   ├── date_utils.py       # Utilitaires de date (daily, catchup...)
│   │   ├── db_utils.py         # Handler JDBC
│   │   ├── io_utils.py         # Fonctions read_data/write_data génériques
│   │   ├── metadata.py         # Gestion des métadonnées (dernier run)
│   │   ├── mongo_utils.py      # Handler MongoDB
│   │   ├── sftp_utils.py       # Handler SFTP
│   │   └── spark.py            # Initialisation SparkSession
│   │
│   ├── jobs/                   # Logique spécifique à chaque job
│   │   ├── __init__.py
│   │   ├── zebra_last_transaction/ # Job Exemple 1
│   │   │   ├── __init__.py
│   │   │   ├── main.py         # Point d'entrée du job
│   │   │   ├── loader.py       # Logique de chargement (utilise io_utils)
│   │   │   ├── transformer.py  # Logique métier principale
│   │   │   └── writer.py       # Logique d'écriture (utilise io_utils)
│   │   │
│   │   └── ...                 # Autres jobs (nouvelle structure ici)
│   │
│   └── __init__.py
│
├── scripts/                    # Scripts utiles
│   └── run_spark_job.sh        # Script de lancement générique via spark-submit
│
├── tests/                      # Tests unitaires/intégration (à développer)
│
├── requirements.txt            # Dépendances Python
├── .gitignore
└── README.md                   # Ce fichier
```

---

## 2. Prérequis

*   **Python:** Version 3.x recommandée.
*   **Spark:** Une installation fonctionnelle de Spark (compatible avec la version PySpark dans `requirements.txt`). L'exécution peut se faire en mode local, standalone, YARN, Kubernetes, etc. (configurable via `scripts/run_spark_job.sh`).
*   **Dépendances Python:** Installer via `pip install -r requirements.txt`.
*   **Drivers JDBC/Connecteurs:** Les drivers nécessaires (ex: MariaDB JDBC, MongoDB Spark Connector) doivent être accessibles par Spark. La méthode recommandée est de les spécifier via l'argument `--jars` ou `--packages` dans `scripts/run_spark_job.sh`.
*   **Accès Réseau:** L'environnement d'exécution Spark doit avoir accès aux bases de données, serveurs SFTP, et systèmes de fichiers (HDFS, S3, local) configurés.

---

## 3. Configuration

La configuration est gérée via des fichiers `.ini` (communs) et `.yaml` (communs et spécifiques aux jobs).

### 3.1 Configuration Commune (`config/common/`)

*   **`db_connections.ini`:** Définit les paramètres de connexion pour les bases de données (JDBC, MongoDB).
    *   Chaque section `[connection_name]` représente une connexion physique.
    *   Utiliser des composants simples (`host`, `port`, `base`, `user`, `pwd`, `db_type`). Ne PAS mettre l'URI complète ici.
    *   Peut inclure des options par défaut (`jdbc_options`, `mongo_options`).
    *   **SÉCURITÉ :** Éviter les mots de passe en clair. Utiliser des variables d'environnement ou des gestionnaires de secrets et adapter `config.py` ou `db_utils.py`/`mongo_utils.py` pour les lire.
*   **`remote_access.ini`:** Définit les connexions SFTP.
    *   Section `[connection_name]`.
    *   Clés : `uri` (hostname/IP), `user`, `pwd` (ou `private_key`), `port` (optionnel).
    *   Peut contenir des options par défaut (`log`, `ciphers`).
*   **`spark_defaults.yaml`:** Définit les configurations Spark par défaut (memory, cores, shuffle partitions, etc.). Ces valeurs peuvent être écrasées par la section `spark_config` d'un job spécifique.

### 3.2 Configuration Spécifique aux Jobs (`config/jobs/`)

*   Chaque job a son propre fichier YAML (ex: `zebra_last_transaction.yaml`). Utiliser `config/config.yaml.template` comme modèle.
*   **Sections Clés :**
    *   `spark_app_name` (Optionnel): Nom de l'application Spark.
    *   `data_sources`: Dictionnaire des sources de données logiques utilisées par le job.
        *   `logical_name:` (Nom choisi pour la source dans ce job)
            *   `connection_name`: Référence à la section dans `db_connections.ini` ou `remote_access.ini`.
            *   `db_type` / `source_type` (Optionnel pour clarté): Aide à l'identification.
            *   `table`/`collection`/`path`: Cible spécifique (table BDD, collection Mongo, chemin fichier).
            *   `options_override` (Optionnel): Dictionnaire pour surcharger les options *comportementales* (non liées à l'accès) de la connexion pour *cette* utilisation spécifique.
    *   `columns` (Optionnel): Mapping des noms de colonnes logiques vers physiques, structuré par nom logique de source.
    *   `business_logic`: Constantes utilisées dans les transformations du job.
    *   `outputs`: Dictionnaire des destinations de sortie logiques (structure similaire à `data_sources`).
        *   `logical_name:`
            *   `output_type`: Type de sortie ('parquet_file', 'jdbc_table', 'mongo_collection', etc.).
            *   `connection_name` (Si BDD/Mongo/SFTP): Référence à `.ini`.
            *   `table`/`collection`/`path`: Destination spécifique.
            *   `mode`: Mode d'écriture Spark ('overwrite', 'append'...).
            *   `partition_by` (Si fichier): Liste des colonnes de partitionnement.
            *   `options_override` (Optionnel): Options d'écriture spécifiques.
    *   `metadata`: Configuration pour le suivi de la dernière date traitée (essentiel pour `daily`/`catchup`). Choisir méthode fichier (`tracker_file_path`) ou BDD (`metadata_db_connection_name`, `tracker_db_table`). Définir un `job_name_identifier` unique.
    *   `spark_config` (Optionnel): Pour surcharger les défauts Spark de `spark_defaults.yaml`.

---

## 4. Exécution des Jobs

### 4.1 Utilisation du Script `run_spark_job.sh`

Le script `scripts/run_spark_job.sh` est le point d'entrée recommandé pour lancer les jobs via `spark-submit`. Il gère :

*   La configuration de l'environnement Spark (master, deploy mode).
*   La transmission des dépendances (JARs JDBC/Mongo via `--jars` ou `--packages`).
*   La configuration des ressources Spark.
*   La construction du `PYTHONPATH`.
*   La transmission des arguments spécifiques au job (mode d'exécution, dates, etc.) au script `main.py` du job.

**Arguments du Script :**

*   `--job-name <nom_job>`: **Obligatoire.** Nom du job à exécuter (correspond au nom du dossier dans `src/jobs/`).
*   `--execution-mode <mode>`: **Obligatoire.** Mode d'exécution : `initial`, `daily`, `catchup`, `specific_date`.
*   `--job-config <fichier.yaml>` (Optionnel): Nom du fichier de configuration dans `config/jobs/`. Par défaut: `<nom_job>.yaml`.
*   `--start-date <YYYY-MM-DD>` (Optionnel): Requis si `execution-mode=initial`.
*   `--end-date <YYYY-MM-DD>` (Optionnel): Requis si `execution-mode=initial`.
*   `--processing-date <YYYY-MM-DD>` (Optionnel): Requis si `execution-mode=specific_date`.

**Avant la Première Exécution :**

1.  **Adapter `run_spark_job.sh`:**
    *   Configurer `SPARK_MASTER` et `SPARK_DEPLOY_MODE` pour votre environnement.
    *   **Configurer `MARIADB_JDBC_DRIVER_PATH`** avec le chemin correct vers le JAR du driver MariaDB.
    *   Si vous utilisez MongoDB ou d'autres BDD, ajouter les packages/jars correspondants (`PACKAGES_ARG` ou `JARS_ARG`).
    *   Ajuster les ressources Spark par défaut (`DEFAULT_*_MEMORY`, etc.).
2.  **Rendre Exécutable:** `chmod +x scripts/run_spark_job.sh`

### 4.2 Exemples de Commandes

*(Exécuter depuis la racine du projet `data_pipelines_project/`)*

*   **Exécuter le traitement quotidien pour `zebra_last_transaction` :**
    ```bash
    ./scripts/run_spark_job.sh --job-name zebra_last_transaction --execution-mode daily
    ```

*   **Exécuter un chargement initial pour `zebra_last_transaction` :**
    ```bash
    ./scripts/run_spark_job.sh --job-name zebra_last_transaction --execution-mode initial --start-date 2023-01-01 --end-date 2023-12-31
    ```

*   **Exécuter un rattrapage manuel pour `zebra_last_transaction` :**
    ```bash
    ./scripts/run_spark_job.sh --job-name zebra_last_transaction --execution-mode catchup
    ```

*   **Exécuter pour une date spécifique :**
    ```bash
    ./scripts/run_spark_job.sh --job-name zebra_last_transaction --execution-mode specific_date --processing-date 2024-03-10
    ```

---

## 5. Jobs Implémentés

### 5.1 zebra_last_transaction
*   [Voir la documentation détaillée](docs/jobs/zebra_last_transaction.md)
*   **Objectif :** Déterminer, pour chaque client ZEBRA actif, la date de sa dernière transaction (C2S, O2C, C2C) et sa catégorie associée.
*   **Configuration :** `config/jobs/zebra_last_transaction.yaml`
*   **Sources Logiques :** `zebra_users` (MariaDB), `zebra_transactions` (MariaDB).
*   **Sortie Logique :** `last_transaction_output` (Fichier Parquet partitionné par jour).
*   **Logique Spécifique :** Voir `src/jobs/zebra_last_transaction/transformer.py`.

---

## 6. Ajouter un Nouveau Job

Suivez ces étapes pour intégrer un nouveau pipeline de traitement :

### 6.1 Structure du Répertoire

1.  Créez un nouveau dossier pour votre job dans `src/jobs/` (ex: `src/jobs/customer_segmentation/`).
2.  À l'intérieur, créez les fichiers Python nécessaires :
    *   `__init__.py` (vide)
    *   `main.py`: Point d'entrée principal pour ce job (similaire à celui de `zebra_last_transaction`, orchestrant les étapes).
    *   `loader.py` (Optionnel mais recommandé): Contient la logique spécifique pour savoir *quelles* données charger en utilisant les noms logiques définis dans le YAML et en appelant `common.io_utils.read_data`.
    *   `transformer.py`: Contient la logique métier principale et les transformations Spark spécifiques à ce job.
    *   `writer.py` (Optionnel mais recommandé): Contient la logique spécifique pour préparer et écrire les différentes sorties logiques en appelant `common.io_utils.write_data`.

### 6.2 Fichier de Configuration

1.  Copiez `config/config.yaml.template` vers `config/jobs/nom_de_votre_job.yaml`.
2.  Modifiez ce nouveau fichier YAML :
    *   Définissez les `data_sources` logiques requises par votre job, en les liant aux connexions physiques (`.ini`) et en spécifiant les tables/collections/chemins. Ajoutez les `options_override` si nécessaire.
    *   (Optionnel) Définissez les mappings `columns`.
    *   Définissez les constantes `business_logic`.
    *   Définissez les `outputs` logiques avec leur type, destination et options.
    *   Configurez la section `metadata` avec un `job_name_identifier` **unique**.
    *   Ajustez `spark_app_name` et `spark_config` si besoin.

### 6.3 Code du Job (`main.py`, `loader.py`, `transformer.py`, `writer.py`)

*   **`main.py`:**
    *   Doit parser les arguments standards (`--config-file`, `--common-config-dir`, `--execution-mode`, dates...).
    *   Charger la configuration via `common.config.load_app_config`.
    *   Initialiser Spark via `common.spark.get_spark_session`.
    *   Déterminer les dates de traitement via `common.date_utils.get_processing_dates`.
    *   Appeler les fonctions de votre `loader.py` pour charger les données (en utilisant les noms logiques des sources).
    *   Appeler les fonctions de votre `transformer.py` pour appliquer la logique métier.
    *   Préparer les DataFrames finaux pour chaque sortie logique.
    *   Appeler les fonctions de votre `writer.py` (ou `io_utils.write_data` directement) pour écrire chaque sortie (en utilisant les noms logiques des sorties).
    *   Mettre à jour les métadonnées via `common.metadata.write_last_processed_date` en cas de succès.
    *   Gérer les erreurs et arrêter Spark proprement (`finally` block).
*   **`loader.py`:**
    *   Contient des fonctions comme `load_source_A(spark, config)`, `load_source_B(spark, config, start_date, end_date)`.
    *   Chaque fonction utilise `common.io_utils.read_data(spark, config, "nom_logique_source", **kwargs)` pour obtenir le DataFrame. Elle peut construire des `query_string` ou `pipeline` spécifiques si nécessaire et les passer via `kwargs`.
*   **`transformer.py`:**
    *   Contient la logique Spark pure (jointures, agrégations, UDFs...) spécifique à ce job. Prend des DataFrames en entrée, retourne des DataFrames transformés.
*   **`writer.py`:**
    *   Contient des fonctions comme `write_output_X(df, config)`, `write_output_Y(df, config)`.
    *   Chaque fonction appelle `common.io_utils.write_data(df, spark, config, "nom_logique_output", **kwargs)` pour écrire le résultat.

### 6.4 Utilisation des Modules Communs

Tirez parti des modules dans `src/common/` pour :

*   Charger la config (`config.py`).
*   Obtenir la session Spark (`spark.py`).
*   Gérer les dates (`date_utils.py`).
*   Lire/écrire les métadonnées (`metadata.py`).
*   Lire/écrire les données de manière générique (`io_utils.py`).
*   (Si besoin de contrôle très fin) Obtenir des handlers spécifiques (`data_clients.py`).

### 6.5 Enregistrement (Optionnel)

Si votre nouveau job nécessite des configurations communes spécifiques (ex: une nouvelle connexion BDD/SFTP), ajoutez la section correspondante dans `config/common/db_connections.ini` ou `config/common/remote_access.ini`.

### 6.6 Exécution du Nouveau Job

Utilisez le script `run_spark_job.sh` en passant le nouveau `--job-name` :

```bash
./scripts/run_spark_job.sh --job-name nom_de_votre_job --execution-mode daily
```

---

## 7. Tests

*(Section à développer)*

Il est fortement recommandé d'ajouter :

*   **Tests Unitaires:** Pour les fonctions complexes dans `transformer.py`, `date_utils.py`, etc., en utilisant `pytest` et potentiellement des mocks ou `pyspark.testing`. Placer dans `tests/jobs/nom_du_job/` et `tests/common/`.
*   **Tests d'Intégration:** Scripts qui exécutent le job complet sur un petit jeu de données de test (dans une BDD/fichier de test) pour vérifier le flux de bout en bout.

---

## 8. Considérations de Sécurité

*   **Mots de Passe:** La méthode actuelle de stockage des mots de passe en clair dans les fichiers `.ini` est **INSECURE**. Mettre en place une solution sécurisée (variables d'environnement, HashiCorp Vault, AWS Secrets Manager, Azure Key Vault, etc.) et adapter le code (principalement `config.py` ou les fonctions `get_..._properties`/`options`) pour les récupérer de manière sécurisée.
*   **Permissions Fichiers:** Assurer que les fichiers de configuration contenant des informations sensibles ont des permissions restreintes.
*   **Clés Hôte SFTP:** Éviter de désactiver la vérification des clés hôte (`cnopts.hostkeys = None`) en production. Utiliser un fichier `known_hosts` correctement géré.
