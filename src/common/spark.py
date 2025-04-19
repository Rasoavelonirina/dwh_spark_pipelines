# src/common/spark.py

import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

def get_spark_session(app_name="DefaultDWHSparkApp", spark_config=None):
    """
    Initializes and returns a SparkSession.

    Args:
        app_name (str): The name for the Spark application.
        spark_config (dict, optional): A dictionary containing Spark configuration
                                         options (e.g., {"spark.driver.memory": "2g"}).
                                         Defaults to None.

    Returns:
        pyspark.sql.SparkSession: The initialized SparkSession instance.

    Raises:
        Exception: If SparkSession creation fails.
    """
    logger.info(f"Attempting to initialize SparkSession with app name: {app_name}")
    try:
        builder = SparkSession.builder.appName(app_name)

        # Apply additional Spark configurations if provided
        if spark_config and isinstance(spark_config, dict):
            for key, value in spark_config.items():
                logger.debug(f"Setting Spark config: {key} = {value}")
                builder = builder.config(key, value)

        # Add configurations often needed for JDBC/database interaction if not already set
        # Example: Ensure necessary JARs are included if not managed externally
        # builder = builder.config("spark.jars.packages", "org.mariadb.jdbc:mariadb-java-client:3.0.8") # Example - adjust version and driver

        # Get or create the SparkSession
        spark = builder.getOrCreate()

        # Log basic Spark context info
        sc = spark.sparkContext
        logger.info(f"SparkSession initialized successfully.")
        logger.info(f"Spark application name: {sc.appName}")
        logger.info(f"Spark version: {sc.version}")
        logger.info(f"Spark master: {sc.master}")
        # Add more relevant context logging if needed

        return spark

    except Exception as e:
        logger.error(f"Failed to initialize SparkSession: {e}", exc_info=True)
        # Re-raise the exception to be caught by the calling script (main.py)
        raise

if __name__ == '__main__':
    # Example of direct execution for testing this module
    print("Testing Spark session creation...")
    try:
        # Example with specific config
        custom_config = {
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:log4j.properties" # Requires log4j.properties
        }
        # test_spark = get_spark_session("TestSparkSession", custom_config)
        # Simple test without extra config:
        test_spark = get_spark_session("TestSparkSession")
        print(f"SparkSession object: {test_spark}")
        print("Successfully created SparkSession.")
        print("Stopping test SparkSession.")
        test_spark.stop()
        print("Test SparkSession stopped.")
    except Exception as e:
        print(f"Error during SparkSession test: {e}")