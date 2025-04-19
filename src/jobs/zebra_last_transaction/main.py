# src/jobs/zebra_last_transaction/main.py

import argparse
import logging
import sys

# Import common modules
try:
    from common import config as config_loader
    from common import spark as spark_initializer
    from common import date_utils
    from common import metadata as common_metadata # Import the common metadata module
    # Import job-specific modules
    from jobs.zebra_last_transaction import loader # Import the loader module
    from jobs.zebra_last_transaction import transformer # Import the transformer module
    from jobs.zebra_last_transaction import writer # Import the writer module
except ImportError as e:
    print(f"Error importing modules: {e}. Ensure __init__.py files exist and PYTHONPATH is set correctly if needed.")
    sys.exit(1)

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Zebra Last Transaction Processor")
    parser.add_argument("--config-file", required=True, help="Path to the job configuration YAML file.")
    parser.add_argument("--common-config-dir", default="config/common", help="Path to the common configuration directory.")
    parser.add_argument("--execution-mode", required=True, choices=['initial', 'daily', 'catchup', 'specific_date'], help="Execution mode.")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD) for 'initial' mode.")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD) for 'initial' mode (inclusive).")
    parser.add_argument("--processing-date", help="Specific date (YYYY-MM-DD) for 'specific_date' mode.")
    return parser.parse_args()

def main():
    """Main execution function."""
    args = parse_arguments()
    logger.info(f"Starting job: Zebra Last Transaction Processor with arguments: {args}")

    spark = None
    latest_processed_date_in_run = None

    try:
        # 1. Load Configuration
        logger.info(f"Loading configuration using job config: {args.config_file} and common dir: {args.common_config_dir}")
        config = config_loader.load_app_config(args.config_file, args.common_config_dir)
        logger.info("Configuration loaded successfully.")


        # 2. Initialize Spark Session
        logger.info("Initializing Spark session using common initializer...")
        app_name = config.get("spark_app_name", "ZebraLastTransaction")
        spark_extra_config = config.get("spark_config", {})
        spark = spark_initializer.get_spark_session(app_name, spark_extra_config)
        logger.info("Spark session initialized via common module.")


        # 3. Determine Processing Dates
        logger.info(f"Determining processing dates for mode: {args.execution_mode}")
        # Appel à date_utils qui utilise maintenant common_metadata
        processing_dates = date_utils.get_processing_dates(
            mode=args.execution_mode,
            config=config, # Passer la config complète
            start_date_str=args.start_date,
            end_date_str=args.end_date,
            processing_date_str=args.processing_date
        )

        if not processing_dates:
            logger.info("No dates to process based on execution mode and metadata. Exiting successfully.")
            sys.exit(0)

        processing_dates.sort()
        logger.info(f"Processing dates determined and sorted: {[d.isoformat() for d in processing_dates]}")
        start_process_date = processing_dates[0]
        end_process_date = processing_dates[-1]
        logger.info(f"Processing dates: {start_process_date.isoformat()} to {end_process_date.isoformat()}")


        # --- Data Processing Steps ---
        # 4. Load Data (USING THE LOADER MODULE)
        logger.info("--- Starting Data Loading ---")
        active_users_df = loader.load_active_users(spark, config)
        transactions_df = loader.load_transactions(spark, config, start_process_date, end_process_date)
        # Optional: Add caching if these DFs are reused multiple times in complex transformations
        # active_users_df.cache()
        # transactions_df.cache()
        logger.info("--- Data Loading Finished ---")
        data_loaded_successfully = True # Assume success if no exception

        # 5. Transform Data (USING THE TRANSFORMER MODULE)
        logger.info("--- Starting Data Transformation ---")
        final_df = transformer.calculate_last_transaction(active_users_df, transactions_df, config)
        # Optional: Cache the final result if it's used multiple times later (e.g., writing + QA checks)
        # final_df.cache()
        logger.info("--- Data Transformation Finished ---")
        transform_successful = True

        # 6. Prepare Final Output Columns
        logger.info("--- Preparing Final Output Columns ---")
        output_day_string = end_process_date.isoformat()
        active_status_value = config.get("business_logic", {}).get("active_status_value", "ACTIF")

        if final_df.rdd.isEmpty():
             logger.warning("Transformed DataFrame is empty. Nothing to write.")
             # If there's nothing to write, consider the write step successful in terms of flow
             write_successful = True
             output_df = None # Ensure output_df is None or an empty DF
        else:
             from pyspark.sql.functions import lit
             output_df = final_df.withColumn("party_id", final_df["msisdn"]) \
                                 .withColumn("zb_user_status", lit(active_status_value)) \
                                 .withColumn("day", lit(output_day_string)) \
                                 .select(
                                     "party_id",
                                     "zb_last_transaction_date",
                                     "zb_user_category",
                                     "zb_user_status",
                                     "day"
                                 )
             logger.info("Final output columns prepared.")
             output_df.printSchema()
             logger.info("--- Final Output Preparation Finished ---")

             # 7. Write Data (USING THE WRITER MODULE)
             if output_df: # Check if there is actually data to write
                 logger.info("--- Starting Data Writing ---")
                 writer.write_output(output_df, config) # Call the writer function
                 # No need for logger message here, writer handles it
                 write_successful = True # Assume success if no exception from writer
             else:
                 # This case is handled above where write_successful is set to True
                 pass # Already logged that nothing is written

        # Set overall success flag
        processing_successful = data_loaded_successfully and transform_successful and write_successful

    except Exception as e:
        logger.error(f"An error occurred during job execution: {e}", exc_info=True)
        processing_successful = False
        # No need to raise e here, finally block will handle exit code based on processing_successful

    finally:
        # 8. Update Metadata
        exit_code = 1 # Default to failure unless success is confirmed
        if processing_successful:
            latest_processed_date_in_run = end_process_date
            logger.info(f"Processing completed successfully up to {latest_processed_date_in_run.isoformat()}.")
            logger.info("Attempting to update metadata...")
            try:
                common_metadata.write_last_processed_date(config, latest_processed_date_in_run)
                logger.info(f"Metadata successfully updated.")
                exit_code = 0 # Success
            except Exception as meta_error:
                logger.error(f"CRITICAL: Processing succeeded but failed to update metadata! Error: {meta_error}", exc_info=True)
                exit_code = 5 # Metadata failure after success
        else:
            logger.warning("Processing did not complete successfully. Metadata will NOT be updated.")
            # Keep exit_code = 1 (or map specific error types to codes if needed)

        # Stop Spark Session
        if spark:
            logger.info("Stopping Spark session.")
            # Optional: Unpersist cached dataframes
            spark.stop()
            logger.info("Spark session stopped.")

        logger.info(f"Job finished with exit code: {exit_code}")
        sys.exit(exit_code)

if __name__ == "__main__":
    main()