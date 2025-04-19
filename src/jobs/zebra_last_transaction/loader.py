# src/jobs/zebra_last_transaction/loader.py

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, upper # Import col and upper functions
from datetime import date

try:
    from common.data_clients import get_data_handler 
    from common.jdbc_utils import JdbcHandler # Importer pour vérification de type si nécessaire
except ImportError:
     print("Error: Could not import get_data_handler or JdbcHandler.")
     get_data_handler = None
     JdbcHandler = None # Définir comme None si l'import échoue

logger = logging.getLogger(__name__)


def load_active_users(spark: SparkSession, config: dict) -> DataFrame:
    """
    Loads the MSISDNs of active users from the master table.

    Args:
        spark (SparkSession): The active SparkSession.
        config (dict): The application configuration.

    Returns:
        DataFrame: A DataFrame containing a single column 'msisdn' with active user MSISDNs.

    Raises:
        ValueError: If configuration is missing required table/column names.
        py4j.protocol.Py4JJavaError: If there's an error connecting to or reading from the database.
    """
    logger.info("Loading active users...")
    if not get_data_handler:
         raise ImportError("Data Handler client factory not available.")

    connection_name = config.get('database_connection_name')
    if not connection_name:
        raise ValueError("Job config missing 'database_connection_name'.")

    # Get the handler instance using the new factory name
    data_handler = get_data_handler(spark, config, connection_name)
    if not data_handler:
        raise RuntimeError(f"Failed to initialize Data Handler for connection '{connection_name}'.")

    # --- Vérifier que c'est bien un handler JDBC pour read_query ---
    # Cette vérification est nécessaire car read_query est spécifique à JdbcHandler
    if not isinstance(data_handler, JdbcHandler):
         raise TypeError(f"Expected a JdbcHandler for connection '{connection_name}' to use read_query, but got {type(data_handler).__name__}.")

    # --- Get config details (inchangé) ---
    users_table_name = config.get("tables", {}).get("users")
    msisdn_col_name = config.get("columns", {}).get("users", {}).get("msisdn", "user_msisdn") # Default added
    status_col_name = config.get("columns", {}).get("users", {}).get("status", "user_status") # Default added
    active_status_value = config.get("business_logic", {}).get("active_status_value")

    if not users_table_name:
        raise ValueError("Config missing 'tables.users' entry.")
    if not active_status_value:
        raise ValueError("Config missing 'business_logic.active_status_value'.")

    logger.info(f"Reading from table: {users_table_name}")
    logger.info(f"Filtering by status column: {status_col_name} = '{active_status_value}'")
    logger.info(f"Selecting MSISDN column: {msisdn_col_name}")

    try:
        # Construct the query to filter directly in the database for efficiency
        # Using 'upper' on both sides for case-insensitive comparison if needed
        # Adjust if your database collation handles case sensitivity correctly
        # Using f-string is generally okay here as table/column names come from trusted config
        # --- Construct and run query
        query_string = f"""
                SELECT {msisdn_col_name} AS msisdn
                FROM {users_table_name}
                WHERE upper({status_col_name}) = upper('{active_status_value}')
            """
        query_alias = "active_users_query"
        # The outer alias `active_users_query` is required by Spark JDBC source

        logger.debug(f"Executing JDBC read with query: {query_string.strip()}")
        # Utiliser la méthode read_query spécifique du JdbcHandler
        active_users_df = data_handler.read_query(query_alias=query_alias, query_string=query_string)

        # Ensure the column name is 'msisdn' for consistency downstream
        active_users_df = active_users_df.select(col("msisdn").alias("msisdn"))

        count = active_users_df.count() # Action to trigger read and check connection
        logger.info(f"Successfully loaded {count} active users.")
        if logger.isEnabledFor(logging.DEBUG):
             active_users_df.printSchema()
             # active_users_df.show(5, truncate=False) # Show sample data in debug

        return active_users_df

    except Exception as e: # Catch Py4J errors and others
        logger.error(f"Failed to load active users from {users_table_name}: {e}", exc_info=True)
        # Re-raise the error to be caught by main.py
        raise


def load_transactions(spark: SparkSession, config: dict, start_date: date, end_date: date) -> DataFrame:
    """
    Loads transactions within the specified date range.

    Args:
        spark (SparkSession): The active SparkSession.
        config (dict): The application configuration.
        start_date (date): The start date for filtering transactions (inclusive).
        end_date (date): The end date for filtering transactions (inclusive).

    Returns:
        DataFrame: A DataFrame containing relevant transaction columns for the period.

    Raises:
        ValueError: If configuration is missing required table/column names.
        py4j.protocol.Py4JJavaError: If there's an error connecting to or reading from the database.
    """
    logger.info(f"Loading transactions from {start_date.isoformat()} to {end_date.isoformat()}...")

    if not get_data_handler:
         raise ImportError("Data Handler client factory not available.")

    connection_name = config.get('database_connection_name')
    if not connection_name:
        raise ValueError("Job config missing 'database_connection_name'.")

    data_handler = get_data_handler(spark, config, connection_name)
    if not data_handler:
        raise RuntimeError(f"Failed to initialize Data Handler for connection '{connection_name}'.")

    # --- Vérifier que c'est bien un handler JDBC ---
    if not isinstance(data_handler, JdbcHandler):
         raise TypeError(f"Expected a JdbcHandler for connection '{connection_name}' to use read_query, but got {type(data_handler).__name__}.")

    # --- Get config details (inchangé) ---
    tx_table_name = config.get("tables", {}).get("transactions")
    date_col_name = config.get("columns", {}).get("transactions", {}).get("date", "tra_date") # Default added
    # Get all needed transaction columns from config
    sndr_msisdn_col = config.get("columns",{}).get("transactions",{}).get("sndr_msisdn", "tra_sndr_msisdn")
    rcvr_msisdn_col = config.get("columns",{}).get("transactions",{}).get("rcvr_msisdn", "tra_receiver_msisdn")
    sndr_cat_col = config.get("columns",{}).get("transactions",{}).get("sndr_category", "tra_sndr_category")
    rcvr_cat_col = config.get("columns",{}).get("transactions",{}).get("rcvr_category", "tra_receiver_category")
    channel_col = config.get("columns",{}).get("transactions",{}).get("channel", "tra_channel")
    # Add other transaction columns if needed later by transformer

    if not tx_table_name:
        raise ValueError("Config missing 'tables.transactions' entry.")

    cols_to_select = [
        (sndr_msisdn_col, "sender_msisdn"),
        (rcvr_msisdn_col, "receiver_msisdn"),
        (date_col_name, "transaction_date"), # Ensure consistent naming
        (sndr_cat_col, "sender_category"),
        (rcvr_cat_col, "receiver_category"),
        (channel_col, "channel")
    ]
    select_expr = ", ".join([f"`{orig}` AS `{alias}`" for orig, alias in cols_to_select])

    logger.info(f"Reading from table: {tx_table_name}")
    logger.info(f"Filtering by date column: {date_col_name} between '{start_date.isoformat()}' and '{end_date.isoformat()}'")
    logger.info(f"Selecting columns: {select_expr}")

    # Option 1: Filter using `where` clause (simpler, relies on DB optimization)
    # date_filter_str = f"{date_col_name} >= '{start_date.isoformat()}' AND {date_col_name} <= '{end_date.isoformat()}'"

    # Option 2: Filter using Spark's pushdown predicates (potentially better for partitioning)
    # Define predicates for Spark to (hopefully) push down to the database
    # This requires the date column in the DB to be DATE or TIMESTAMP type
    # Format for predicate pushdown depends on the driver/db - check Spark JDBC docs
    # Example assuming date column is DATE:
    # predicates = [
    #     f"{date_col_name} >= date'{start_date.isoformat()}'",
    #     f"{date_col_name} <= date'{end_date.isoformat()}'"
    # ]
    # If TIMESTAMP:
    predicates = [
        f"{date_col_name} >= timestamp'{start_date.isoformat()} 00:00:00'",
        f"{date_col_name} <= timestamp'{end_date.isoformat()} 23:59:59'"
    ]

    # Construct the query using selected columns
    # Ensure backticks or quotes around column names if they contain special chars or are keywords
    query_string = f"""
        (SELECT {select_expr}
         FROM {tx_table_name}
         WHERE {predicates[0]} AND {predicates[1]}
        )
    """
    query_alias = "transactions_query" # Alias for the subquery
    # The WHERE clause here is primarily for documentation/readability;
    # the actual pushdown might happen via predicates if option 2 is used fully.
    # Using WHERE directly is often reliable. Let's stick to explicit WHERE for now.

    logger.debug(f"Executing JDBC read with query: {query_string.strip()}")
    try:
        transactions_df = data_handler.read_query(query_alias=query_alias, query_string=query_string)
        count = transactions_df.count() # Action to trigger read and check connection
        logger.info(f"Successfully loaded {count} transactions for the period.")

        if logger.isEnabledFor(logging.DEBUG):
             transactions_df.printSchema()
             # transactions_df.show(5, truncate=False) # Show sample data in debug

        return transactions_df

    except Exception as e: # Catch Py4J errors and others
        logger.error(f"Failed to load transactions from {tx_table_name}: {e}", exc_info=True)
        # Re-raise the error to be caught by main.py
        raise

# --- Example usage for testing ---
if __name__ == '__main__':
    # This test requires a running SparkSession and valid config files.
    # It's better tested via integration tests or by running the main job.
    import os
    from datetime import timedelta
    print("Testing loader functions (requires SparkSession and config)...")

    # Create a dummy SparkSession for basic testing (if not running via spark-submit)
    try:
        spark_session = SparkSession.builder.appName("LoaderTest").master("local[*]").getOrCreate() # Use local mode for testing
        print("Dummy SparkSession created.")

        # --- !!! IMPORTANT !!! ---
        # You MUST have valid config files at these paths for the test to run
        # AND the database must be accessible from where you run this test.
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__))) # Adjust based on your structure
        common_config_dir = os.path.join(repo_root, 'config/common')
        job_config_path = os.path.join(repo_root, 'config/jobs/zebra_last_transaction.yaml')

        if not os.path.exists(job_config_path) or not os.path.exists(common_config_dir):
             print("ERROR: Config files not found. Cannot run loader test.")
        else:
            print("Loading configuration...")
            from common import config as config_loader # Need config loader
            test_config = config_loader.load_app_config(job_config_path, common_config_dir)
            print("Configuration loaded.")

            # 1. Test loading active users
            print("\n--- Testing load_active_users ---")
            try:
                active_df = load_active_users(spark_session, test_config)
                print(f"Loaded {active_df.count()} active users.")
                active_df.show(5)
            except Exception as e:
                print(f"Error loading active users: {e}")

            # 2. Test loading transactions for a period (e.g., yesterday)
            print("\n--- Testing load_transactions (yesterday) ---")
            test_end_date = date.today() - timedelta(days=1)
            test_start_date = test_end_date # Just load for one day
            try:
                tx_df = load_transactions(spark_session, test_config, test_start_date, test_end_date)
                print(f"Loaded {tx_df.count()} transactions for {test_start_date.isoformat()}.")
                tx_df.show(5)
            except Exception as e:
                print(f"Error loading transactions: {e}")


        print("\nStopping SparkSession.")
        spark_session.stop()

    except Exception as main_e:
        print(f"An error occurred during the test setup or execution: {main_e}")

    print("\nLoader tests finished.")