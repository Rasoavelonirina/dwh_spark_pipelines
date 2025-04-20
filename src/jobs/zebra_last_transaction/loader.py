# src/jobs/zebra_last_transaction/loader.py

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, upper # Keep col, upper might not be needed if query does it
from pyspark.sql.types import StringType, StructType, StructField, TimestampType
from datetime import date

# Importer la fonction générique de lecture
try:
    from common import data_clients 
except ImportError as e:
     logging.error(f"Failed to import io_utils module: {e}", exc_info=True)
     # Définir une fonction dummy pour permettre au code de charger mais échouer à l'exécution
     def read_data(*args, **kwargs):
         raise ImportError("io_utils module not loaded correctly.")
     io_utils = type('obj', (object,), {'read_data': read_data})()


logger = logging.getLogger(__name__)

# Noms logiques des sources utilisées par CE job (doivent correspondre au YAML)
LOGICAL_SOURCE_USERS = "zebra_users"
LOGICAL_SOURCE_TRANSACTIONS = "zebra_transactions"


def load_active_users(spark: SparkSession, config: dict) -> DataFrame:
    """
    Loads the MSISDNs of active users using the generic io_utils.read_data function.
    Constructs a specific SQL query for filtering at the source.

    Args:
        spark (SparkSession): The active SparkSession.
        config (dict): The application configuration.

    Returns:
        DataFrame: A DataFrame containing a single column 'msisdn'.
                   Returns an empty DataFrame with 'msisdn' schema if read fails or returns None.

    Raises:
        ValueError: If required configuration keys for query construction are missing.
        Exception: Propagates exceptions from io_utils.read_data if reading fails critically.
    """
    logger.info(f"Loading active users from logical source: '{LOGICAL_SOURCE_USERS}'")

    # --- 1. Get configuration details needed for the query ---
    source_config = config.get('data_sources', {}).get(LOGICAL_SOURCE_USERS, {})
    users_table_name = source_config.get('table') # Table name from the logical source config
    if not users_table_name:
        raise ValueError(f"Missing 'table' definition for logical source '{LOGICAL_SOURCE_USERS}'.")

    column_mapping = config.get('columns', {}).get(LOGICAL_SOURCE_USERS, {})
    msisdn_col_name = column_mapping.get("msisdn", "user_msisdn")
    status_col_name = column_mapping.get("status", "user_status")

    active_status_value = config.get("business_logic", {}).get("active_status_value")
    if not active_status_value:
        raise ValueError("Missing 'business_logic.active_status_value'.")
    # --- End config details ---

    # --- 2. Construct the specific SQL query ---
    # Using backticks for potential reserved words or special characters in names
    # Filtering and aliasing done directly in SQL for efficiency (predicate pushdown guaranteed)
    query_string = f"""
        SELECT `{msisdn_col_name}` AS msisdn
        FROM {users_table_name}
        WHERE upper(`{status_col_name}`) = upper('{active_status_value}')
    """
    query_alias = f"{LOGICAL_SOURCE_USERS}_query" # Alias required by Spark JDBC when using query

    # --- 3. Call the generic reader ---
    try:
        logger.info(f"Calling io_utils.read_data with query for '{LOGICAL_SOURCE_USERS}'")
        active_users_df = data_clients.read_data(
            spark,
            config,
            LOGICAL_SOURCE_USERS,
            # Pass query specific arguments via kwargs
            query_string=query_string,
            query_alias=query_alias
            # Options d'override (ex: fetchsize) sont gérées dans io_utils via la config
        )

        # --- 4. Post-read validation and selection ---
        if active_users_df is None:
             logger.warning(f"Read operation for '{LOGICAL_SOURCE_USERS}' returned None. Returning empty DataFrame.")
             # Créer un DF vide avec le schéma attendu
             return spark.createDataFrame([], "msisdn string")

        # Ensure the column exists and select only it
        if "msisdn" not in active_users_df.columns:
             logger.error(f"Query for '{LOGICAL_SOURCE_USERS}' did not return expected 'msisdn' column.")
             # Log schema for debugging
             active_users_df.printSchema()
             raise ValueError("Active users query result missing 'msisdn' column.")

        final_df = active_users_df.select(col("msisdn"))

        count = final_df.count() # Action pour vérifier
        logger.info(f"Successfully loaded {count} active users from '{LOGICAL_SOURCE_USERS}'.")
        return final_df

    except Exception as e:
        logger.error(f"Failed to load active users from '{LOGICAL_SOURCE_USERS}': {e}", exc_info=True)
        # Optionnel: retourner un DF vide au lieu de crasher ?
        # logger.warning(f"Returning empty DataFrame due to error loading active users.")
        # return spark.createDataFrame([], "msisdn string")
        # Ou propager l'erreur (comportement actuel)
        raise


def load_transactions(spark: SparkSession, config: dict, start_date: date, end_date: date) -> DataFrame:
    """
    Loads transactions within the specified date range using the generic io_utils.read_data function.
    Constructs a specific SQL query for filtering at the source.

    Args:
        spark (SparkSession): The active SparkSession.
        config (dict): The application configuration.
        start_date (date): The start date for filtering transactions (inclusive).
        end_date (date): The end date for filtering transactions (inclusive).

    Returns:
        DataFrame: A DataFrame containing relevant transaction columns.
                   Returns an empty DataFrame with expected schema if read fails or returns None.

    Raises:
        ValueError: If required configuration keys for query construction are missing.
        Exception: Propagates exceptions from io_utils.read_data if reading fails critically.
    """
    logger.info(f"Loading transactions for '{LOGICAL_SOURCE_TRANSACTIONS}' from {start_date.isoformat()} to {end_date.isoformat()}")
    if not io_utils: raise ImportError("io_utils module not available.")

    # --- 1. Get configuration details needed for the query ---
    source_config = config.get('data_sources', {}).get(LOGICAL_SOURCE_TRANSACTIONS, {})
    tx_table_name = source_config.get('table')
    if not tx_table_name:
        raise ValueError(f"Missing 'table' definition for logical source '{LOGICAL_SOURCE_TRANSACTIONS}'.")

    column_mapping = config.get('columns', {}).get(LOGICAL_SOURCE_TRANSACTIONS, {})
    date_col_name = column_mapping.get("date", "tra_date")
    sndr_msisdn_col = column_mapping.get("sndr_msisdn", "tra_sndr_msisdn")
    rcvr_msisdn_col = column_mapping.get("rcvr_msisdn", "tra_receiver_msisdn")
    sndr_cat_col = column_mapping.get("sndr_category", "tra_sndr_category")
    rcvr_cat_col = column_mapping.get("rcvr_category", "tra_receiver_category")
    channel_col = column_mapping.get("channel", "tra_channel")
    # --- End config details ---

    # --- 2. Construct the specific SQL query ---
    cols_to_select = [
        (sndr_msisdn_col, "sender_msisdn"),
        (rcvr_msisdn_col, "receiver_msisdn"),
        (date_col_name, "transaction_date"),
        (sndr_cat_col, "sender_category"),
        (rcvr_cat_col, "receiver_category"),
        (channel_col, "channel")
    ]
    select_expr = ", ".join([f"`{orig}` AS `{alias}`" for orig, alias in cols_to_select])
    # Assuming date column is timestamp or comparable type
    date_filter = f"`{date_col_name}` >= timestamp'{start_date.isoformat()} 00:00:00' AND `{date_col_name}` <= timestamp'{end_date.isoformat()} 23:59:59'"

    query_string = f"""
        SELECT {select_expr}
        FROM {tx_table_name}
        WHERE {date_filter}
    """
    query_alias = f"{LOGICAL_SOURCE_TRANSACTIONS}_query"

    # --- 3. Call the generic reader ---
    try:
        logger.info(f"Calling io_utils.read_data with query for '{LOGICAL_SOURCE_TRANSACTIONS}'")
        transactions_df = data_clients.read_data(
            spark,
            config,
            LOGICAL_SOURCE_TRANSACTIONS,
            query_string=query_string,
            query_alias=query_alias
        )

        # --- 4. Post-read validation ---
        if transactions_df is None:
             logger.warning(f"Read operation for '{LOGICAL_SOURCE_TRANSACTIONS}' returned None. Returning empty DataFrame.")
             # Définir le schéma attendu pour le DataFrame vide
             expected_schema = StructType([
                 StructField("sender_msisdn", StringType(), True),
                 StructField("receiver_msisdn", StringType(), True),
                 StructField("transaction_date", TimestampType(), True), # Ou DateType selon la source
                 StructField("sender_category", StringType(), True),
                 StructField("receiver_category", StringType(), True),
                 StructField("channel", StringType(), True)
             ])
             return spark.createDataFrame([], expected_schema)

        # Vérifier si les colonnes attendues sont présentes (optionnel mais recommandé)
        expected_cols = {alias for _, alias in cols_to_select}
        actual_cols = set(transactions_df.columns)
        if not expected_cols.issubset(actual_cols):
             logger.error(f"Query for '{LOGICAL_SOURCE_TRANSACTIONS}' did not return all expected columns. Missing: {expected_cols - actual_cols}")
             transactions_df.printSchema()
             raise ValueError("Transaction query result missing expected columns.")

        count = transactions_df.count() # Action pour vérifier
        logger.info(f"Successfully loaded {count} transactions from '{LOGICAL_SOURCE_TRANSACTIONS}'.")
        return transactions_df

    except Exception as e:
        logger.error(f"Failed to load transactions from '{LOGICAL_SOURCE_TRANSACTIONS}': {e}", exc_info=True)
        # Optionnel: retourner un DF vide
        # logger.warning(f"Returning empty DataFrame due to error loading transactions.")
        # expected_schema = ... # Définir schema comme ci-dessus
        # return spark.createDataFrame([], expected_schema)
        # Ou propager l'erreur
        raise