# src/common/data_clients.py
import logging
from pyspark.sql import SparkSession

# Import helpers and handlers from other common modules
try:
    from common.base_data_handler import BaseDataHandler
    from common.jdbc_utils import JdbcHandler, get_db_connection_properties
    from common.mongo_utils import MongoHandler, get_mongo_connection_options
    from common.sftp_utils import SftpHandler # Assuming sftp_utils exists
except ImportError as e:
     logging.error(f"Failed to import handler modules: {e}", exc_info=True)
     # Define dummies so subsequent calls fail clearly
     BaseDataHandler = None
     JdbcHandler = None
     MongoHandler = None
     SftpHandler = None
     get_db_connection_properties = None
     get_mongo_connection_options = None

logger = logging.getLogger(__name__)

def get_data_handler(spark: SparkSession, config: dict, connection_name: str) -> BaseDataHandler | None:
    """
    Factory function to get a configured Data Handler instance (JDBC or Mongo).

    Args:
        spark (SparkSession): The active SparkSession.
        config (dict): The application configuration.
        connection_name (str): The name of the connection (from db_connections.ini).

    Returns:
        BaseDataHandler: An initialized handler instance (JdbcHandler or MongoHandler).
        None: If configuration is missing, invalid, or handler cannot be created.
    """
    logger.info(f"Requesting Data Handler for connection: '{connection_name}'")

    # Vérifier que les imports ont réussi
    if not BaseDataHandler or not JdbcHandler or not MongoHandler or not get_db_connection_properties or not get_mongo_connection_options:
         logger.error("Handler modules or helpers failed to import. Cannot create handler.")
         return None

    try:
        db_connections = config.get('db_connections')
        if not db_connections or connection_name not in db_connections:
            raise ValueError(f"Connection details for '{connection_name}' not found in 'db_connections' config.")

        conn_details = db_connections[connection_name]
        db_type = conn_details.get('db_type', 'jdbc') # Default to jdbc if not specified

        logger.info(f"Determined db_type as '{db_type}' for connection '{connection_name}'.")

        # --- Instancier le bon handler ---
        if db_type.lower() in ['mariadb', 'mysql', 'postgres', 'sqlserver', 'jdbc']: # Add other JDBC types as needed
            jdbc_url, properties = get_db_connection_properties(config, connection_name)
            return JdbcHandler(spark, connection_name, jdbc_url, properties)

        elif db_type.lower() == 'mongodb':
            mongo_options = get_mongo_connection_options(config, connection_name)
            return MongoHandler(spark, connection_name, mongo_options)

        else:
            raise ValueError(f"Unsupported db_type '{db_type}' specified for connection '{connection_name}'.")

    except ValueError as e:
        logger.error(f"Failed to configure Data Handler for '{connection_name}': {e}", exc_info=True)
        return None
    except Exception as e:
         logger.error(f"Unexpected error creating Data Handler for '{connection_name}': {e}", exc_info=True)
         return None

def get_sftp_handler(config: dict, connection_name: str) -> SftpHandler | None:
    """
    Factory function to get a configured SftpHandler instance.

    Args:
        config (dict): The application configuration.
        connection_name (str): The name of the SFTP connection (from remote_access.ini).

    Returns:
        SftpHandler: An initialized SftpHandler instance ready for use with 'with'.
        None: If configuration is missing or invalid.
    """
    logger.info(f"Requesting SftpHandler for connection: '{connection_name}'")
    try:
        remote_access_config = config.get('remote_access')
        if not remote_access_config or connection_name not in remote_access_config:
            raise ValueError(f"SFTP connection details for '{connection_name}' not found in common 'remote_access' config.")

        conn_details = remote_access_config[connection_name]

        host = conn_details.get('uri') # Assuming 'uri' is the hostname/IP
        port = int(conn_details.get('port', 22)) # Default SFTP port is 22
        user = conn_details.get('user')
        pwd = conn_details.get('pwd') # SECURITY WARNING!
        # Add support for private key files if needed
        # pkey_path = conn_details.get('private_key_path')

        if not host or not user:
            raise ValueError(f"Missing 'uri' or 'user' for SFTP connection '{connection_name}'.")
        # Password/Key validation is done inside SftpHandler constructor

        # TODO: Add logic to load custom CnOpts if specified in config
        # custom_cnopts = CNOPTS # Use default from sftp_utils for now

        return SftpHandler(
            host=host,
            port=port,
            username=user,
            password=pwd,
            # private_key=pkey_path, # Pass if using key auth
            # cnopts=custom_cnopts
        )
    except ValueError as e:
        logger.error(f"Failed to configure SftpHandler for '{connection_name}': {e}", exc_info=True)
        return None
    except Exception as e:
         logger.error(f"Unexpected error creating SftpHandler for '{connection_name}': {e}", exc_info=True)
         return None