# src/common/base_data_handler.py

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession

class BaseDataHandler(ABC):
    """Abstract base class for data handlers (JDBC, MongoDB, etc.)."""

    def __init__(self, spark: SparkSession, connection_name: str):
        if not spark:
            raise ValueError("SparkSession is required.")
        self.spark = spark
        self.connection_name = connection_name # Store the name for reference/logging

    @abstractmethod
    def read(self, target: str, options: dict = None) -> DataFrame:
        """
        Reads data from the specified target (table or collection).

        Args:
            target (str): The name of the table (for JDBC) or collection (for MongoDB).
            options (dict, optional): Additional options for the read operation.

        Returns:
            DataFrame: The resulting Spark DataFrame.
        """
        pass

    @abstractmethod
    def write(self, df: DataFrame, target: str, mode: str = 'overwrite', options: dict = None):
        """
        Writes a DataFrame to the specified target (table or collection).

        Args:
            df (DataFrame): The DataFrame to write.
            target (str): The name of the table (for JDBC) or collection (for MongoDB).
            mode (str, optional): Spark write mode ('append', 'overwrite', 'ignore', 'error').
            options (dict, optional): Additional options for the write operation.
        """
        pass

    # Optionnel: Ajouter des méthodes plus spécifiques si nécessaire
    # @abstractmethod
    # def execute_query(self, ...): # Pourrait être spécifique JDBC
    #     pass
    # @abstractmethod
    # def read_pipeline(self, ...): # Pourrait être spécifique Mongo
    #     pass