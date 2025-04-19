# src/common/db_utils.py

import logging
from pyspark.sql import SparkSession, DataFrame

# Importer la classe de base
try:
    from common.base_data_handler import BaseDataHandler
except ImportError:
    # Fallback basic si l'import échoue (moins propre)
    BaseDataHandler = object
    print("Warning: Could not import BaseDataHandler. Inheritance might not work as expected.")

logger = logging.getLogger(__name__)

# --- Fonction Helper (Déplacée et Généralisée depuis loader.py) ---
def get_db_connection_properties(config: dict, connection_name: str) -> tuple[str, dict]:
    """
    Extracts database connection details for a given connection name from the configuration.

    Args:
        config (dict): The application configuration dictionary.
        connection_name (str): The name of the connection section in 'db_connections'.

    Returns:
        tuple: (jdbc_url, connection_properties) where connection_properties is a dict
               containing user, password, driver, and potentially other options.

    Raises:
        ValueError: If required configuration keys are missing.
    """
    # ... (Get db_connections and conn_details as before) ...
    db_connections = config.get('db_connections')
    if not db_connections or connection_name not in db_connections:
        raise ValueError(f"Connection details for '{connection_name}' not found.")
    conn_details = db_connections[connection_name]

    # --- Extraire les composants ---
    db_type = conn_details.get('db_type', 'jdbc') # Default to generic jdbc
    host = conn_details.get('host')
    port_str = conn_details.get('port') # Port est optionnel
    base = conn_details.get('base')
    user = conn_details.get('user')
    password = conn_details.get('pwd')
    jdbc_options = conn_details.get('jdbc_options', '') # Options supplémentaires

    # --- Validation des composants requis ---
    if not host: raise ValueError(f"Missing 'host' for JDBC connection '{connection_name}'.")
    if not base: raise ValueError(f"Missing 'base' (database name) for JDBC connection '{connection_name}'.")
    if not user: raise ValueError(f"Missing 'user' for JDBC connection '{connection_name}'.")
    if password is None: raise ValueError(f"Missing 'pwd' (password) for JDBC connection '{connection_name}'.")

    # --- Déterminer Port et Driver par défaut ---
    default_ports = {'mariadb': 3306, 'mysql': 3306, 'postgres': 5432, 'sqlserver': 1433}
    default_drivers = {
        'mariadb': 'org.mariadb.jdbc.Driver',
        'mysql': 'com.mysql.cj.jdbc.Driver', # Ou com.mysql.jdbc.Driver pour anciennes versions
        'postgres': 'org.postgresql.Driver',
        'sqlserver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
    }
    port = port_str if port_str else default_ports.get(db_type.lower())
    driver = conn_details.get('driver', default_drivers.get(db_type.lower()))

    if not port: logger.warning(f"Could not determine default port for db_type '{db_type}'. Specify 'port' in config.")
    # Port might still be None if db_type unknown and port not specified
    port_str_for_url = f":{port}" if port else ""

    if not driver: raise ValueError(f"Missing 'driver' and could not determine default for db_type '{db_type}' for connection '{connection_name}'.")

    # --- Construire l'URL JDBC ---
    # Format: jdbc:<db_type>://<host>:<port>/<base>?<options>
    jdbc_url = f"jdbc:{db_type}://{host}{port_str_for_url}/{base}"
    if jdbc_options:
        # Assurer que les options commencent par '?'
        if not jdbc_options.startswith('?'):
             jdbc_options = '?' + jdbc_options
        jdbc_url += jdbc_options
    logger.debug(f"Constructed JDBC URL for '{connection_name}': {jdbc_url}")

    # --- Préparer les propriétés ---
    properties = {
        "user": user,
        "password": password,
        "driver": driver
    }
    custom_props = conn_details.get('properties') # Garder la possibilité de props additionnels
    if custom_props and isinstance(custom_props, dict):
        properties.update(custom_props)
        logger.debug(f"Added custom JDBC properties for '{connection_name}': {custom_props}")

    return jdbc_url, properties

# --- Classe Handler ---
class JdbcHandler(BaseDataHandler): # Hérite de BaseDataHandler
    """
    A wrapper for performing relational database operations using Spark JDBC.
    """
    def __init__(self, spark: SparkSession, connection_name: str, jdbc_url: str, connection_properties: dict):
        # Appeler le constructeur parent
        super().__init__(spark, connection_name)

        if not jdbc_url:
            raise ValueError("JDBC URL is required.")
        if not connection_properties or not connection_properties.get("user"):
             raise ValueError("Connection properties (including user) are required.")

        self.jdbc_url = jdbc_url
        self.properties = connection_properties
        logger.info(f"JdbcHandler initialized for connection '{connection_name}' at URL: {self.jdbc_url}")

    # Implémenter la méthode read de la classe de base
    def read(self, target: str, options: dict = None) -> DataFrame:
        """
        Reads an entire database table (target) into a Spark DataFrame using JDBC.

        Args:
            target (str): The name of the database table.
            options (dict, optional): Additional JDBC options (e.g., fetchsize, queryTimeout).

        Returns:
            DataFrame: The resulting Spark DataFrame.
        """
        logger.info(f"Reading JDBC table: {target} from {self.jdbc_url}")
        read_options = {**self.properties, **(options or {})}
        try:
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=target,
                properties=read_options
            )
            logger.info(f"Successfully read JDBC table {target}.")
            return df
        except Exception as e:
            logger.error(f"Failed to read JDBC table {target}: {e}", exc_info=True)
            raise

    # Garder read_query comme méthode spécifique JDBC
    def read_query(self, query_alias: str, query_string: str, options: dict = None) -> DataFrame:
        """Reads data using a custom SQL query into a Spark DataFrame via JDBC."""
        if not query_alias:
             raise ValueError("query_alias is required for Spark JDBC reads via query.")

        query_string_for_jdbc = f"({query_string.strip()}) AS {query_alias}"
        logger.debug(f"Using query string for JDBC: {query_string_for_jdbc}")

        logger.info(f"Reading JDBC data via query (alias: {query_alias}) from {self.jdbc_url}")
        read_options = {**self.properties, **(options or {})}

        try:
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=query_string_for_jdbc,
                properties=read_options
            )
            logger.info(f"Successfully read JDBC data via query '{query_alias}'.")
            return df
        except Exception as e:
            logger.error(f"Failed to read JDBC via query '{query_alias}': {e}", exc_info=True)
            raise

    # Implémenter la méthode write de la classe de base
    def write(self, df: DataFrame, target: str, mode: str = 'overwrite', options: dict = None):
        """
        Writes a Spark DataFrame to a database table (target) using JDBC.

        Args:
            df (DataFrame): The DataFrame to write.
            target (str): The name of the database table.
            mode (str, optional): Spark write mode. Defaults to 'overwrite'.
            options (dict, optional): Additional JDBC options (e.g., batchsize, truncate).
        """
        logger.info(f"Writing DataFrame via JDBC to table: {target} at {self.jdbc_url} (mode: {mode})")
        write_options = {**self.properties, **(options or {})}
        try:
            df.write.jdbc(
                url=self.jdbc_url,
                table=target,
                mode=mode,
                properties=write_options
            )
            logger.info(f"Successfully wrote DataFrame via JDBC to table {target}.")
        except Exception as e:
            logger.error(f"Failed to write DataFrame via JDBC to table {target}: {e}", exc_info=True)
            raise