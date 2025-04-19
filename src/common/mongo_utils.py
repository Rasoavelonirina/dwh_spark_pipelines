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

# --- Fonction Helper pour extraire les options Mongo ---
def get_mongo_connection_options(config: dict, connection_name: str) -> dict:
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

    db_connections = config.get('db_connections')
    if not db_connections or connection_name not in db_connections:
        raise ValueError(f"MongoDB connection details for '{connection_name}' not found in common 'db_connections' config.")

    conn_details = db_connections[connection_name]

    if conn_details.get('db_type') != 'mongodb':
         logger.warning(f"Connection '{connection_name}' is not of db_type 'mongodb'.")
         # Or raise ValueError depending on strictness

    mongo_uri = conn_details.get('uri')
    database_name = conn_details.get('base')

    if not mongo_uri:
        raise ValueError(f"Missing 'uri' (MongoDB connection string) for connection '{connection_name}'.")
    if not database_name:
        raise ValueError(f"Missing 'base' (MongoDB database name) for connection '{connection_name}'.")

    # Start with essential options
    options = {
        "uri": mongo_uri,
        "database": database_name
    }

    # Add other potential options from config (e.g., readPreference, readConcern)
    # These need to match the Spark MongoDB connector option names
    # Example: Mapping 'readPreference' from config if present
    if 'readPreference' in conn_details:
         options['readPreference.name'] = conn_details['readPreference']
    if 'readConcern' in conn_details:
         options['readConcern.level'] = conn_details['readConcern']
    # Add more options as needed...

    logger.debug(f"Prepared MongoDB options for connection '{connection_name}': {options}")
    return options


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