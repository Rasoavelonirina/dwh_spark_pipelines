# src/jobs/zebra_last_transaction/transformer.py

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

def calculate_last_transaction(active_users_df: DataFrame, transactions_df: DataFrame, config: dict) -> DataFrame:
    """
    Calculates the last transaction date and associated category for each active user,
    based on transaction channel rules.

    Args:
        active_users_df (DataFrame): DataFrame with one column 'msisdn' for active users.
        transactions_df (DataFrame): DataFrame with loaded transaction data, including columns like
                                     'sender_msisdn', 'receiver_msisdn', 'transaction_date',
                                     'sender_category', 'receiver_category', 'channel'.
        config (dict): The application configuration dictionary.

    Returns:
        DataFrame: A DataFrame with columns:
                   - msisdn: The user's MSISDN.
                   - zb_last_transaction_date: The date of the user's last relevant transaction.
                   - zb_user_category: The user's category associated with that last transaction.
    """
    logger.info("--- Starting Transformation: Calculating Last Transaction ---")

    # --- 1. Prepare Data & Get Constants ---
    # Ensure input dataframes are not empty if possible (or handle downstream)
    if active_users_df.rdd.isEmpty():
        logger.warning("Active users DataFrame is empty. No results will be produced.")
        # Return an empty dataframe with the expected schema
        spark = SparkSession.getActiveSession()
        if spark:
             schema = "msisdn string, zb_last_transaction_date date, zb_user_category string"
             return spark.createDataFrame([], schema)
        else:
             # Should not happen if called from main.py, but as safeguard
             raise RuntimeError("SparkSession not available to create empty DataFrame.")

    if transactions_df.rdd.isEmpty():
        logger.warning("Transactions DataFrame is empty for the period. No results will be produced.")
        # Can return empty DF matching active_users schema or expected output schema
        spark = SparkSession.getActiveSession()
        if spark:
             schema = "msisdn string, zb_last_transaction_date date, zb_user_category string"
             return spark.createDataFrame([], schema)
        else:
             raise RuntimeError("SparkSession not available to create empty DataFrame.")


    # Get channel constants from config for reliable comparison
    channel_c2s = config.get("business_logic", {}).get("channel_c2s", "C2S").upper()
    channel_o2c = config.get("business_logic", {}).get("channel_o2c", "O2C").upper()
    channel_c2c = config.get("business_logic", {}).get("channel_c2c", "C2C").upper()
    logger.debug(f"Using channel constants: C2S='{channel_c2s}', O2C='{channel_o2c}', C2C='{channel_c2c}'")

    # --- 2. Unify Transaction Events ---
    # Create a unified view of relevant events (msisdn, date, category) per channel type

    # C2S: Sender is the relevant party
    c2s_events = transactions_df.where(F.upper(F.col("channel")) == channel_c2s) \
                                .select(
                                    F.col("sender_msisdn").alias("msisdn"),
                                    F.col("transaction_date").alias("event_date"),
                                    F.col("sender_category").alias("event_category")
                                )
    logger.debug("Created C2S events intermediate DataFrame.")

    # O2C: Receiver is the relevant party
    o2c_events = transactions_df.where(F.upper(F.col("channel")) == channel_o2c) \
                                .select(
                                    F.col("receiver_msisdn").alias("msisdn"),
                                    F.col("transaction_date").alias("event_date"),
                                    F.col("receiver_category").alias("event_category")
                                )
    logger.debug("Created O2C events intermediate DataFrame.")

    # C2C: Both Sender and Receiver are relevant parties - create two records per C2C transaction
    c2c_base = transactions_df.where(F.upper(F.col("channel")) == channel_c2c)

    c2c_sender_events = c2c_base.select(
                                    F.col("sender_msisdn").alias("msisdn"),
                                    F.col("transaction_date").alias("event_date"),
                                    F.col("sender_category").alias("event_category")
                                )

    c2c_receiver_events = c2c_base.select(
                                    F.col("receiver_msisdn").alias("msisdn"),
                                    F.col("transaction_date").alias("event_date"),
                                    F.col("receiver_category").alias("event_category")
                                )
    logger.debug("Created C2C sender and receiver events intermediate DataFrames.")

    # Combine all event types using unionByName (robust to column order)
    all_potential_events = c2s_events.unionByName(o2c_events) \
                                     .unionByName(c2c_sender_events) \
                                     .unionByName(c2c_receiver_events)

    # Filter out events where msisdn might be null or empty (if possible in source data)
    all_potential_events = all_potential_events.where(F.col("msisdn").isNotNull() & (F.col("msisdn") != ""))
    logger.info("Combined events from all relevant channels.")
    if logger.isEnabledFor(logging.DEBUG):
        all_potential_events.printSchema()
        logger.debug(f"Count of potential events before filtering by active users: {all_potential_events.count()}") # Action


    # --- 3. Filter Events by Active Users ---
    # Keep only events related to users currently active
    # Use a broadcast hint if active_users_df is significantly smaller than all_potential_events
    active_user_events = all_potential_events.join(
        F.broadcast(active_users_df), # Broadcast hint for potentially smaller DF
        all_potential_events["msisdn"] == active_users_df["msisdn"],
        "inner"
    ).select(all_potential_events["msisdn"], "event_date", "event_category") # Select columns from the events DF

    logger.info("Filtered events to include only currently active users.")
    if logger.isEnabledFor(logging.DEBUG):
        active_user_events.printSchema()
        logger.debug(f"Count of events for active users: {active_user_events.count()}") # Action
        # active_user_events.show(5, truncate=False)


    # --- 4. Find the Latest Event per User ---
    # Use Window functions to rank events per user by date and pick the latest one

    # Define the window specification: partition by user, order by date descending
    window_spec = Window.partitionBy("msisdn").orderBy(F.col("event_date").desc())

    # Add a row number to each event within its user partition
    ranked_events = active_user_events.withColumn("rn", F.row_number().over(window_spec))

    logger.debug("Ranked events per user by date.")

    # Keep only the latest event (rank = 1) for each user
    latest_events = ranked_events.where(F.col("rn") == 1)
    logger.info("Selected the latest event for each active user.")


    # --- 5. Final Selection and Renaming ---
    # Select the required columns and rename them to match the desired output schema

    final_df = latest_events.select(
        F.col("msisdn"), # Keep msisdn as is for now (will be renamed later)
        F.col("event_date").alias("zb_last_transaction_date"),
        F.col("event_category").alias("zb_user_category") # Category from the latest transaction
    )

    logger.info("Transformation complete. Final DataFrame schema:")
    final_df.printSchema()
    if logger.isEnabledFor(logging.DEBUG):
         logger.debug(f"Count of users with a last transaction found: {final_df.count()}") # Action
         # final_df.show(5, truncate=False)

    logger.info("--- Transformation Finished ---")
    return final_df


# --- Example usage for testing (Conceptual) ---
if __name__ == '__main__':
    # Direct testing of this transformer requires creating SparkSession and dummy DataFrames.
    # This is often done in dedicated integration test files.
    print("Transformer module loaded. Direct execution requires test setup.")

    # Example of how you *might* set up a test:
    # from pyspark.sql import SparkSession
    # from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType

    # spark = SparkSession.builder.appName("TransformerTest").master("local[*]").getOrCreate()

    # # Dummy Active Users
    # active_users_data = [("111",), ("222",), ("444",)]
    # active_users_schema = StructType([StructField("msisdn", StringType(), True)])
    # active_df = spark.createDataFrame(active_users_data, active_users_schema)

    # # Dummy Transactions
    # tx_data = [
    #     ("111", "333", date(2024, 3, 10), "CAT_A", "CAT_B", "C2S"), # User 111 is active sender
    #     ("555", "222", date(2024, 3, 11), "CAT_C", "CAT_D", "O2C"), # User 222 is active receiver
    #     ("111", "222", date(2024, 3, 12), "CAT_A", "CAT_D", "C2C"), # Both active, C2C
    #     ("111", "666", date(2024, 3, 9),  "CAT_A", "CAT_E", "C2S"), # Earlier tx for 111
    #     ("777", "444", date(2024, 3, 13), "CAT_F", "CAT_G", "O2C"), # User 444 active receiver
    #     ("888", "999", date(2024, 3, 14), "CAT_H", "CAT_I", "C2C"), # Neither user active
    # ]
    # tx_schema = StructType([
    #     StructField("sender_msisdn", StringType(), True),
    #     StructField("receiver_msisdn", StringType(), True),
    #     StructField("transaction_date", DateType(), True), # Use DateType or TimestampType matching loader
    #     StructField("sender_category", StringType(), True),
    #     StructField("receiver_category", StringType(), True),
    #     StructField("channel", StringType(), True),
    # ])
    # tx_df = spark.createDataFrame(tx_data, tx_schema)

    # # Dummy Config
    # test_config = {
    #     "business_logic": {
    #         "channel_c2s": "C2S",
    #         "channel_o2c": "O2C",
    #         "channel_c2c": "C2C"
    #     }
    # }

    # print("\n--- Running calculate_last_transaction with dummy data ---")
    # try:
    #     result_df = calculate_last_transaction(active_df, tx_df, test_config)
    #     print("Result Schema:")
    #     result_df.printSchema()
    #     print("Result Data:")
    #     result_df.show(truncate=False)
    #     # Expected Output:
    #     # +------+--------------------------+----------------+
    #     # |msisdn|zb_last_transaction_date|zb_user_category|
    #     # +------+--------------------------+----------------+
    #     # |111   |2024-03-12                |CAT_A           | C2C Sender, latest for 111
    #     # |222   |2024-03-12                |CAT_D           | C2C Receiver, latest for 222
    #     # |444   |2024-03-13                |CAT_G           | O2C Receiver, latest for 444
    #     # +------+--------------------------+----------------+
    # except Exception as e:
    #      print(f"Error during test transformation: {e}")

    # spark.stop()