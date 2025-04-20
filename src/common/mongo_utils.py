# src/common/mongo_utils.py

import logging
from pyspark.sql import SparkSession, DataFrame

# Importer la classe de base
try:
    from common.base_data_handler import BaseDataHandler
except ImportError:
    BaseDataHandler = object
    print("Warning: Could not import BaseDataHandler. Inheritance might not work as expected.")

logger = logging.getLogger(__name__)

# Liste (non exhaustive) d'options de connexion Mongo NON overrideables par les jobs
MONGO_CORE_CONNECTION_KEYS = {'uri', 'host', 'port', 'base', 'user', 'pwd', 'authSource', 'replicaSet', 'db_type', 'mongo_options'}
# Options du connecteur Spark connues (pour référence)
# Voir: https://www.mongodb.com/docs/spark-connector/current/configuration/

# --- Fonction Helper pour extraire les options Mongo ---
def get_mongo_connection_options(config: dict, connection_name: str, overrides: dict = None) -> dict:
    """
    Extracts MongoDB connection options for a given connection name from the configuration.

    Args:
        config (dict): The application configuration dictionary.
        connection_name (str): The name of the connection section in 'db_connections'.

    Returns:
        dict: A dictionary containing options for the Spark MongoDB connector,
              including 'uri', 'database'. Collection is NOT included here.

    Raises:
        ValueError: If required configuration keys are missing.
    """

    if not connection_name:
        raise ValueError("MongoDB connection name cannot be empty.")
    if overrides is None:
        overrides = {}

    # ... (Get db_connections and conn_details as before) ...
    db_connections = config.get('db_connections')
    if not db_connections or connection_name not in db_connections:
        raise ValueError(f"Connection details for '{connection_name}' not found.")
    conn_details = db_connections[connection_name]

    # --- Extraire les composants ---
    db_type = conn_details.get('db_type')
    if db_type != 'mongodb': raise ValueError(f"Connection '{connection_name}' is not db_type 'mongodb'.")

    host_str = conn_details.get('host')
    port_str = conn_details.get('port', '27017') # Port par défaut Mongo
    base = conn_details.get('base')
    user = conn_details.get('user') # Optionnel
    password = conn_details.get('pwd') # Optionnel (mais requis si user fourni)
    auth_source = conn_details.get('auth_source') # Optionnel
    replica_set = conn_details.get('replica_set') # Optionnel
    mongo_options_str = conn_details.get('mongo_options', '') # Optionnel

    # --- Validation ---
    if not host_str: raise ValueError(f"Missing 'host' for MongoDB connection '{connection_name}'.")
    if not base: raise ValueError(f"Missing 'base' (database name) for MongoDB connection '{connection_name}'.")
    if user and password is None: logger.warning(f"User '{user}' provided for MongoDB connection '{connection_name}' but 'pwd' is missing.")

    # --- Construire l'URI MongoDB ---
    # Format: mongodb://[username:password@]host1[:port1][,host2[:port2],...][/database][?options]

    uri = "mongodb://"

    # Ajouter User/Password si fournis
    if user and password is not None:
        from urllib.parse import quote_plus # Pour encoder user/pwd si caractères spéciaux
        uri += f"{quote_plus(user)}:{quote_plus(password)}@"

    # Ajouter Hôte(s) et Port(s)
    hosts = host_str.split(',')
    host_port_parts = []
    for h in hosts:
        h = h.strip()
        if ':' in h: # Si port est inclus dans la chaîne host
            host_port_parts.append(h)
        else: # Utiliser le port par défaut/spécifié
            host_port_parts.append(f"{h}:{port_str}")
    uri += ",".join(host_port_parts)

    # Ajouter Base (techniquement optionnel dans URI si spécifié via option, mais incluons-le)
    uri += f"/{base}"

    # Ajouter Options
    options_list = []
    if replica_set: options_list.append(f"replicaSet={replica_set}")
    if auth_source: options_list.append(f"authSource={auth_source}")
    # Ajouter les options de la chaîne mongo_options_str
    if mongo_options_str:
        # Nettoyer la chaîne (enlever '?' du début si présent)
        clean_options = mongo_options_str.strip()
        if clean_options.startswith('?'):
            clean_options = clean_options[1:]
        if clean_options: # Ne pas ajouter si vide après nettoyage
             options_list.append(clean_options)

    if options_list:
        uri += "?" + "&".join(options_list)

    logger.debug(f"Constructed MongoDB URI for '{connection_name}': {uri}")

    # --- Préparer le dictionnaire d'options pour Spark ---
    # Le connecteur a principalement besoin de l'URI maintenant
    options_for_spark = {
        "uri": uri,
        "database": base # Redondant si dans URI, mais peut être utile
        # Ajouter d'autres options de base si nécessaire ici, ex:
        # "readPreference.name": conn_details.get('readPreference'), # Exemple
    }

    # Appliquer les overrides (provenant du YAML job)
    # Ces overrides doivent utiliser les noms d'options du connecteur Spark
    applied_overrides = {}
    if overrides and isinstance(overrides, dict):
        for key, value in overrides.items():
             # Sécurité: Vérifier si on tente d'écraser une clé de base (moins pertinent ici car URI est roi)
            if key.lower() in MONGO_CORE_CONNECTION_KEYS:
                 logger.warning(f"Attempted to override core connection key '{key}' via overrides for '{connection_name}'. Override ignored.")
            else:
                 # Ajouter/Écraser les options comportementales pour le connecteur Spark
                 options_for_spark[key] = value
                 applied_overrides[key] = value
        if applied_overrides:
             logger.info(f"Applied connection option overrides for '{connection_name}': {applied_overrides}")
        else:
             logger.debug(f"No applicable options overrides found for '{connection_name}'.")

    return options_for_spark


# --- Classe Handler MongoDB ---
class MongoHandler(BaseDataHandler):
    """
    A wrapper for performing MongoDB operations using the Spark MongoDB connector.
    """
    def __init__(self, spark: SparkSession, connection_name: str, mongo_options: dict):
        """
        Initializes the handler.

        Args:
            spark (SparkSession): The active SparkSession.
            connection_name (str): The logical name of the connection.
            mongo_options (dict): Dictionary of options for the Spark MongoDB connector,
                                  MUST include 'uri' and 'database'.
        """
        super().__init__(spark, connection_name)
        if not mongo_options or not mongo_options.get("uri") or not mongo_options.get("database"):
            raise ValueError("mongo_options must include 'uri' and 'database'.")

        self.mongo_options = mongo_options
        logger.info(f"MongoHandler initialized for connection '{connection_name}' (DB: {mongo_options['database']})")

    def read(self, target: str, options: dict = None) -> DataFrame:
        """
        Reads data from a MongoDB collection (target).

        Args:
            target (str): The name of the MongoDB collection.
            options (dict, optional): Additional options for the read operation
                                      (e.g., pipeline, schema).

        Returns:
            DataFrame: The resulting Spark DataFrame.
        """
        if not target:
            raise ValueError("Target collection name cannot be empty for MongoDB read.")
        logger.info(f"Reading MongoDB collection: {target} from database {self.mongo_options['database']}")

        read_options = {**self.mongo_options, "collection": target, **(options or {})}
        logger.debug(f"Using final read options: {read_options}")

        try:
            df = self.spark.read.format("mongo").options(**read_options).load()
            logger.info(f"Successfully read MongoDB collection '{target}'.")
            return df
        except Exception as e:
            # Catch potential Py4J errors if connector not found or config issues
            if "java.lang.ClassNotFoundException" in str(e) and "com.mongodb.spark.sql" in str(e):
                 logger.error("MongoDB Spark Connector not found! Ensure '--packages org.mongodb.spark:mongo-spark-connector...' is included in spark-submit.")
            logger.error(f"Failed to read MongoDB collection '{target}': {e}", exc_info=True)
            raise

    def read_pipeline(self, target: str, pipeline: list | str, options: dict = None) -> DataFrame:
         """
         Reads data from a MongoDB collection (target) using an aggregation pipeline.

         Args:
             target (str): The name of the MongoDB collection.
             pipeline (list | str): Aggregation pipeline stages as a list of dicts or a JSON string.
             options (dict, optional): Additional options for the read operation.

         Returns:
             DataFrame: The resulting Spark DataFrame.
         """
         if not target:
             raise ValueError("Target collection name cannot be empty for MongoDB read.")
         if not pipeline:
             raise ValueError("Aggregation pipeline cannot be empty.")
         logger.info(f"Reading MongoDB collection: {target} using aggregation pipeline.")

         # Convert pipeline list to JSON string if necessary
         import json
         if isinstance(pipeline, list):
             pipeline_str = json.dumps(pipeline)
         elif isinstance(pipeline, str):
             pipeline_str = pipeline # Assume it's already a valid JSON string
         else:
             raise TypeError("Pipeline must be a list of stages or a JSON string.")

         read_options = {**self.mongo_options, "collection": target, "pipeline": pipeline_str, **(options or {})}
         logger.debug(f"Using final read options with pipeline: {read_options}")

         try:
             df = self.spark.read.format("mongo").options(**read_options).load()
             logger.info(f"Successfully read MongoDB collection '{target}' using pipeline.")
             return df
         except Exception as e:
             if "java.lang.ClassNotFoundException" in str(e) and "com.mongodb.spark.sql" in str(e):
                  logger.error("MongoDB Spark Connector not found! Ensure '--packages org.mongodb.spark:mongo-spark-connector...' is included in spark-submit.")
             logger.error(f"Failed to read MongoDB collection '{target}' with pipeline: {e}", exc_info=True)
             raise


    def write(self, df: DataFrame, target: str, mode: str = 'overwrite', options: dict = None):
        """
        Writes a Spark DataFrame to a MongoDB collection (target).

        Args:
            df (DataFrame): The DataFrame to write.
            target (str): The name of the MongoDB collection.
            mode (str, optional): Spark write mode ('append', 'overwrite'). Defaults to 'overwrite'.
            options (dict, optional): Additional options for the write operation.
        """
        if not target:
            raise ValueError("Target collection name cannot be empty for MongoDB write.")
        logger.info(f"Writing DataFrame to MongoDB collection: {target} in database {self.mongo_options['database']} (mode: {mode})")

        write_options = {**self.mongo_options, "collection": target, **(options or {})}
        logger.debug(f"Using final write options: {write_options}")

        try:
            df.write.format("mongo").mode(mode).options(**write_options).save()
            logger.info(f"Successfully wrote DataFrame to MongoDB collection '{target}'.")
        except Exception as e:
            if "java.lang.ClassNotFoundException" in str(e) and "com.mongodb.spark.sql" in str(e):
                 logger.error("MongoDB Spark Connector not found! Ensure '--packages org.mongodb.spark:mongo-spark-connector...' is included in spark-submit.")
            logger.error(f"Failed to write DataFrame to MongoDB collection '{target}': {e}", exc_info=True)
            raise