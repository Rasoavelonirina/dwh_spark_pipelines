# src/jobs/zebra_last_transaction/writer.py

import logging
from pyspark.sql import DataFrame, SparkSession

# Importer la fonction générique
try:
    from common import data_clients
except ImportError:
     logging.error("Failed to import io_utils module.", exc_info=True)
     # Définir une fonction dummy pour échouer proprement si appelé
     def write_data(*args, **kwargs):
         raise ImportError("io_utils module not loaded correctly.")
     io_utils = type('obj', (object,), {'write_data': write_data})()

logger = logging.getLogger(__name__)

def write_output(output_df: DataFrame, config: dict, logical_output_name: str):
    """
    Writes the output DataFrame using the generic io_utils.write_data function.

    Args:
        output_df (DataFrame): The DataFrame to write.
        config (dict): The application configuration dictionary.
        logical_output_name (str): The logical name of the output destination
                                   defined in the job YAML's 'outputs' section.
    """
    logger.debug(f"Passing call to generic writer for output '{logical_output_name}'.")
    spark = SparkSession.getActiveSession()
    if not spark:
         # Ceci ne devrait pas arriver si appelé depuis main.py
         raise RuntimeError("No active SparkSession found in job writer.")

    try:
        # Simplement appeler la fonction générique
        data_clients.write_data(
            df=output_df,
            spark=spark,
            config=config,
            logical_output_name=logical_output_name
            # kwargs peuvent être passés depuis main si nécessaire, mais généralement config suffit
        )
    except Exception as e:
        # L'erreur est déjà loguée dans io_utils, on la propage
        logger.error(f"Generic writer failed for output '{logical_output_name}'.")
        raise # Propager l'erreur à main.py