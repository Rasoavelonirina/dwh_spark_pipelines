# src/jobs/zebra_last_transaction/main.py

import argparse
import logging
import sys
from datetime import date

# Import common modules
try:
    from common import config as config_loader
    from common import spark as spark_initializer
    from common import date_utils
    from common import metadata as common_metadata # Import the common metadata module
    # Placeholders for loader, transformer, writer
    # from jobs.zebra_last_transaction import loader, transformer, writer
except ImportError as e:
    print(f"Error importing modules: {e}. Ensure __init__.py files exist and PYTHONPATH is set correctly if needed.")
    sys.exit(1)

# Configuration du logging
# ... (pas de changement ici) ...
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    # ... (pas de changement ici) ...
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
        # ... (pas de changement ici) ...
        logger.info(f"Loading configuration using job config: {args.config_file} and common dir: {args.common_config_dir}")
        config = config_loader.load_app_config(args.config_file, args.common_config_dir)
        logger.info("Configuration loaded successfully.")


        # 2. Initialize Spark Session
        # ... (pas de changement ici) ...
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


        # --- Data Processing Steps (Placeholders) ---
        # ... (Placeholders restent les mêmes) ...
        logger.warning("Data Loading step is a placeholder.")
        logger.info(f"Placeholder: Would load data between {start_process_date.isoformat()} and {end_process_date.isoformat()}")
        data_loaded_successfully = True

        logger.warning("Data Transformation step is a placeholder.")
        transform_successful = True

        logger.warning("Data Writing step is a placeholder.")
        write_successful = True


        # 7. Update Metadata (USING common_metadata)
        if data_loaded_successfully and transform_successful and write_successful:
             latest_processed_date_in_run = end_process_date
             logger.info(f"All processing steps successful for dates up to {latest_processed_date_in_run.isoformat()}.")
             logger.info("Attempting to update metadata using common module...")
             try:
                 # Utilise la fonction publique du module commun
                 common_metadata.write_last_processed_date(config, latest_processed_date_in_run)
                 logger.info(f"Metadata successfully updated with last processed date: {latest_processed_date_in_run.isoformat()}")
             except Exception as meta_error:
                 # La fonction write_last_processed_date relève maintenant l'erreur si elle se produit
                 logger.error(f"CRITICAL: Processing succeeded but failed to update metadata! Error: {meta_error}", exc_info=True)
                 # Relever l'erreur pour que le job échoue si la màj des métadata échoue
                 raise RuntimeError("Metadata update failed after successful processing.") from meta_error
        else:
             logger.warning("One or more processing steps failed. Metadata will NOT be updated.")
             raise RuntimeError("Processing steps failed. Job did not complete successfully.")


        logger.info("Job finished successfully.")

    except FileNotFoundError as e:
         logger.error(f"Configuration file not found: {e}", exc_info=True)
         sys.exit(2)
    except ValueError as e:
         logger.error(f"Configuration or Argument Error: {e}", exc_info=True)
         sys.exit(3)
    except ImportError as e: # Attrape aussi l'erreur si common.metadata ne peut être importé
         logger.error(f"Module Import Error: {e}", exc_info=True)
         sys.exit(4)
    except RuntimeError as e:
         logger.error(f"Job Execution Failed: {e}", exc_info=True)
         sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred during the job execution: {e}", exc_info=True)
        sys.exit(1)

    finally:
        if spark:
            logger.info("Stopping Spark session.")
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()