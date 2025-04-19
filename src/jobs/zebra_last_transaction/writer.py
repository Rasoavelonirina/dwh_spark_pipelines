# src/jobs/zebra_last_transaction/writer.py

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F # Potentially needed for repartitioning

logger = logging.getLogger(__name__)

def write_output(output_df: DataFrame, config: dict):
    """
    Writes the final DataFrame to the specified output path,
    partitioned by the 'day' column, in Parquet format.

    Args:
        output_df (DataFrame): The DataFrame to write, containing columns:
                                'party_id', 'zb_last_transaction_date',
                                'zb_user_category', 'zb_user_status', 'day'.
        config (dict): The application configuration dictionary, containing
                       the 'output' section with 'path', 'format',
                       'partition_by', and 'mode'.

    Raises:
        ValueError: If required output configuration is missing.
        Exception: Any Spark exception during the write operation.
    """
    logger.info("--- Starting Data Writing ---")

    output_config = config.get("output")
    if not output_config:
        raise ValueError("Configuration is missing 'output' section.")

    output_path = output_config.get("path")
    output_format = output_config.get("format", "parquet") # Default to parquet
    partition_column = output_config.get("partition_by", "day") # Default partition column
    write_mode = output_config.get("mode", "overwrite") # Default write mode

    if not output_path:
        raise ValueError("Output configuration is missing 'path'.")
    if not partition_column:
        raise ValueError("Output configuration is missing 'partition_by'.")
    if output_format.lower() != "parquet":
        logger.warning(f"Output format specified as '{output_format}'. Parquet is strongly recommended for performance.")
        # Allow other formats but Parquet is standard for data lakes

    # Validate write mode
    valid_modes = ["append", "overwrite", "ignore", "error", "errorifexists"] # Spark standard modes
    if write_mode not in valid_modes:
        logger.warning(f"Invalid write mode '{write_mode}'. Defaulting to 'overwrite'. Valid modes are: {valid_modes}")
        write_mode = "overwrite"

    logger.info(f"Writing output DataFrame to path: {output_path}")
    logger.info(f"Output format: {output_format}")
    logger.info(f"Partitioning by column: {partition_column}")
    logger.info(f"Write mode: {write_mode}")

    # Check if DataFrame is empty before attempting to write
    # isEmpty() can be expensive, count() might trigger computation but is often acceptable here
    # Let's rely on the check done in main.py before calling the writer for simplicity here.
    # If output_df could potentially be empty when passed here, add:
    # if output_df.rdd.isEmpty():
    #     logger.warning("Input DataFrame is empty. Nothing to write.")
    #     logger.info("--- Data Writing Finished (No data) ---")
    #     return # Exit successfully

    try:
        # --- Optional: Repartitioning before write ---
        # Repartitioning can help control the number of output files,
        # especially after joins or shuffles. Adjust number based on data size and needs.
        # Use partition_column to potentially improve write performance if data is skewed.
        # num_output_partitions = spark.conf.get("spark.sql.shuffle.partitions", 200) # Example: use shuffle partitions
        # logger.info(f"Repartitioning data to approximately {num_output_partitions} partitions before writing.")
        # output_df = output_df.repartition(int(num_output_partitions), F.col(partition_column))
        # OR repartition just by number:
        # output_df = output_df.repartition(10) # Example: Write 10 files per partition

        # Write the DataFrame
        writer = output_df.write.format(output_format) \
                                .partitionBy(partition_column) \
                                .mode(write_mode)

        # Add options if needed (e.g., compression for Parquet)
        # writer = writer.option("compression", "snappy") # Default for Parquet usually

        logger.info(f"Executing write operation...")
        writer.save(output_path)

        logger.info(f"Successfully wrote data to: {output_path}")
        logger.info("--- Data Writing Finished ---")

    except Exception as e:
        logger.error(f"Failed to write data to {output_path}: {e}", exc_info=True)
        # Re-raise the exception to be caught by main.py
        raise


# --- Example usage for testing (Conceptual) ---
if __name__ == '__main__':
    # Direct testing requires SparkSession, dummy DataFrame, and config.
    # Best tested via integration tests or running the main job.
    print("Writer module loaded. Direct execution requires test setup.")

    # Example Test Setup (if you were to run it):
    # from pyspark.sql import SparkSession
    # from pyspark.sql.types import StructType, StructField, StringType, DateType
    # from datetime import date
    # import os
    # import shutil

    # spark = SparkSession.builder.appName("WriterTest").master("local[*]").getOrCreate()

    # # Dummy Data matching expected input schema for writer
    # data = [
    #     ("111", date(2024, 3, 12), "CAT_A", "ACTIF", "2024-03-15"),
    #     ("222", date(2024, 3, 12), "CAT_D", "ACTIF", "2024-03-15"),
    #     ("444", date(2024, 3, 13), "CAT_G", "ACTIF", "2024-03-15"),
    #     ("555", date(2024, 3, 14), "CAT_X", "ACTIF", "2024-03-16"), # Different day for partition test
    # ]
    # schema = StructType([
    #     StructField("party_id", StringType(), True),
    #     StructField("zb_last_transaction_date", DateType(), True),
    #     StructField("zb_user_category", StringType(), True),
    #     StructField("zb_user_status", StringType(), True),
    #     StructField("day", StringType(), True), # Partition column is string
    # ])
    # df_to_write = spark.createDataFrame(data, schema)

    # # Dummy Config
    # test_output_dir = "./temp_writer_output"
    # test_config = {
    #     "output": {
    #         "path": test_output_dir,
    #         "format": "parquet",
    #         "partition_by": "day",
    #         "mode": "overwrite"
    #     }
    # }

    # # Clean up previous output
    # if os.path.exists(test_output_dir):
    #      shutil.rmtree(test_output_dir)

    # print(f"\n--- Running write_output with dummy data to: {test_output_dir} ---")
    # try:
    #     write_output(df_to_write, test_config)
    #     print("Write successful. Check directory structure.")
    #     # Verify output (e.g., list files, read back data)
    #     if os.path.exists(test_output_dir):
    #         print("Output directory contents:")
    #         for root, dirs, files in os.walk(test_output_dir):
    #              print(f"  {root} : {dirs} : {files}")
    #         # Try reading back one partition
    #         # try:
    #         #     read_df = spark.read.parquet(os.path.join(test_output_dir, "day=2024-03-15"))
    #         #     print("\nRead back partition day=2024-03-15:")
    #         #     read_df.show()
    #         # except Exception as read_e:
    #         #      print(f"Could not read back partition: {read_e}")
    # except Exception as e:
    #     print(f"Error during test write: {e}")

    # spark.stop()