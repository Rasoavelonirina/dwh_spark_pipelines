# src/common/metadata.py

import json
import os
import logging
from datetime import date, datetime, timedelta
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
logger = logging.getLogger(__name__)

# --- Constantes ---
DATE_FORMAT = "%Y-%m-%d" # Format ISO pour stocker la date

# --- Interface Abstraite pour le Tracker ---
class BaseMetadataTracker(ABC):
    """Abstract base class for different metadata tracking implementations."""

    @abstractmethod
    def read_last_processed_date(self, job_identifier):
        """Reads the last successfully processed date for a given job."""
        pass

    @abstractmethod
    def write_last_processed_date(self, job_identifier, process_date):
        """Writes the successfully processed date for a given job."""
        pass

# --- Implémentation Fichier JSON ---
class JsonFileMetadataTracker(BaseMetadataTracker):
    """Tracks metadata using a JSON file, storing dates per job_identifier."""

    def __init__(self, file_path):
        self.file_path = file_path
        self._metadata_cache = None # Simple cache to avoid repeated file reads
        logger.info(f"Initializing JsonFileMetadataTracker with file: {self.file_path}")

    def _load_metadata(self):
        """Loads the entire metadata file into cache."""
        if not os.path.exists(self.file_path):
            logger.warning(f"Metadata file not found at {self.file_path}. Initializing empty metadata.")
            self._metadata_cache = {}
            return self._metadata_cache

        # Check if cache is already populated
        if self._metadata_cache is not None:
             # Optional: Could add logic here to check file modification time
             # if file is potentially updated by another process. For simplicity, assume cache is valid.
             return self._metadata_cache

        logger.debug(f"Loading metadata from file: {self.file_path}")
        try:
            with open(self.file_path, 'r') as f:
                self._metadata_cache = json.load(f)
                if not isinstance(self._metadata_cache, dict):
                    logger.error(f"Metadata file {self.file_path} does not contain a valid JSON object (dictionary). Resetting.")
                    self._metadata_cache = {} # Reset if file format is wrong
            return self._metadata_cache
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from metadata file {self.file_path}: {e}. Initializing empty metadata.", exc_info=True)
            self._metadata_cache = {}
            return self._metadata_cache
        except IOError as e:
            logger.error(f"Error reading metadata file {self.file_path}: {e}. Cannot proceed.", exc_info=True)
            # If we can't read, raise error as we can't guarantee correct state
            raise IOError(f"Failed to read metadata file: {self.file_path}") from e
        except Exception as e:
            logger.error(f"Unexpected error loading metadata file {self.file_path}: {e}", exc_info=True)
            raise # Re-raise unexpected errors

    def _save_metadata(self):
        """Saves the current metadata cache to the JSON file."""
        if self._metadata_cache is None:
             logger.warning("Metadata cache is not loaded. Cannot save.")
             # Could try loading first, or just return. Let's return.
             return # Or raise an error?

        logger.debug(f"Saving metadata to file: {self.file_path}")
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            with open(self.file_path, 'w') as f:
                json.dump(self._metadata_cache, f, indent=4)
            logger.info(f"Successfully saved metadata to {self.file_path}")
        except IOError as e:
            logger.error(f"Error writing metadata file {self.file_path}: {e}", exc_info=True)
            raise # Re-raise IO error
        except Exception as e:
            logger.error(f"Unexpected error saving metadata file {self.file_path}: {e}", exc_info=True)
            raise # Re-raise unexpected errors

    def read_last_processed_date(self, job_identifier):
        """Reads the last successfully processed date for the given job identifier."""
        if not job_identifier:
            raise ValueError("job_identifier cannot be empty.")

        metadata = self._load_metadata()
        job_data = metadata.get(job_identifier, {})
        last_date_str = job_data.get("last_successfully_processed_date")

        if not last_date_str:
            logger.info(f"No last processed date found for job '{job_identifier}' in {self.file_path}.")
            return None

        try:
            last_date = datetime.strptime(last_date_str, DATE_FORMAT).date()
            logger.info(f"Found last processed date for job '{job_identifier}': {last_date.isoformat()}")
            return last_date
        except ValueError:
            logger.error(f"Invalid date format '{last_date_str}' for job '{job_identifier}' in {self.file_path}. Expected {DATE_FORMAT}.")
            return None # Treat invalid date as missing

    def write_last_processed_date(self, job_identifier, process_date):
        """Writes the successfully processed date for the given job identifier."""
        if not job_identifier:
            raise ValueError("job_identifier cannot be empty.")
        if not isinstance(process_date, date):
            raise ValueError("process_date must be a valid datetime.date object.")

        date_str = process_date.strftime(DATE_FORMAT)
        logger.info(f"Updating last processed date for job '{job_identifier}' to '{date_str}' in {self.file_path}")

        metadata = self._load_metadata() # Ensure cache is loaded

        # Update the specific job's data
        if job_identifier not in metadata:
            metadata[job_identifier] = {}
        metadata[job_identifier]["last_successfully_processed_date"] = date_str
        metadata[job_identifier]["last_update_timestamp"] = datetime.now().isoformat() # Add timestamp

        # Save the entire updated metadata structure
        self._save_metadata()


# --- Factory Function pour créer le bon Tracker ---
def get_metadata_tracker(config):
    """
    Factory function to create the appropriate metadata tracker based on config.

    Args:
        config (dict): The application configuration dictionary. It should contain
                       a 'metadata' section specifying the tracker type and settings.

    Returns:
        BaseMetadataTracker: An instance of a metadata tracker.

    Raises:
        ValueError: If the configuration is invalid or tracker type is unsupported.
    """
    metadata_config = config.get("metadata")
    if not metadata_config:
        raise ValueError("Configuration is missing 'metadata' section.")

    # Determine tracker type (example: prioritize file path)
    tracker_type = None
    if metadata_config.get("tracker_file_path"):
        tracker_type = "json_file"
    elif metadata_config.get("tracker_db_table") and metadata_config.get("metadata_db_connection_name"):
        tracker_type = "database" # Placeholder for future DB implementation
    else:
        raise ValueError("Metadata config must specify either ('tracker_db_table' and 'metadata_db_connection_name') OR 'tracker_file_path'.")

    logger.info(f"Metadata tracker type determined as: {tracker_type}")

    if tracker_type == "json_file":
        file_path = metadata_config.get("tracker_file_path")
        if not file_path: # Should have been checked above, but double check
            raise ValueError("Missing 'tracker_file_path' for json_file metadata tracker.")
        return JsonFileMetadataTracker(file_path)

    elif tracker_type == "database":
        spark = SparkSession.getActiveSession()
        if not spark:
             # Ceci ne devrait pas arriver si appelé depuis un job Spark actif
             raise RuntimeError("No active SparkSession found. DatabaseMetadataTracker requires an active session.")

        db_connection_name = metadata_config.get("metadata_db_connection_name")
        table_name = metadata_config.get("tracker_db_table")
        if not db_connection_name or not table_name: # Validation redondante mais sûre
            raise ValueError("Database tracker requires 'metadata_db_connection_name' and 'tracker_db_table' in metadata config.")

        # Extraire les propriétés de la connexion BDD depuis la config principale
        db_connections = config.get('db_connections')
        if not db_connections or db_connection_name not in db_connections:
            raise ValueError(f"Database connection details for '{db_connection_name}' not found in common 'db_connections' config.")
        conn_details = db_connections[db_connection_name]

        # Préparer les propriétés pour JDBC (similaire à la fonction dans loader.py)
        # On pourrait factoriser _get_db_connection_properties si on veut
        jdbc_url = conn_details.get('uri')
        base_name = conn_details.get('base')
        db_type = conn_details.get('db_type', 'mariadb')
        if not jdbc_url.startswith("jdbc:"):
             if base_name:
                 jdbc_url = f"jdbc:{db_type}://{jdbc_url}/{base_name}"
             else:
                 raise ValueError(f"Database name ('base') required for DB metadata connection '{db_connection_name}'.")

        db_properties = {
            "url": jdbc_url, # Stocker l'URL complet ici pour la classe
            "user": conn_details.get('user'),
            "password": conn_details.get('pwd'), # Rappel: Sécurité!
            "driver": conn_details.get('driver', "org.mariadb.jdbc.Driver")
        }
        custom_props = conn_details.get('properties')
        if custom_props and isinstance(custom_props, dict):
            db_properties.update(custom_props)
        if not db_properties["user"] or db_properties["password"] is None or not db_properties["driver"]:
             raise ValueError(f"Missing user, pwd, or driver for metadata DB connection '{db_connection_name}'.")

        return DatabaseMetadataTracker(spark, db_properties, table_name)

    else:
        # Should not be reachable if logic above is correct
        raise ValueError(f"Unsupported metadata tracker type: {tracker_type}")

# --- NOUVELLE Implémentation Base de Données ---
class DatabaseMetadataTracker(BaseMetadataTracker):
    """Tracks metadata using a database table via Spark JDBC."""

    # Colonnes attendues dans la table BDD
    JOB_ID_COL = "job_identifier"
    DATE_COL = "last_successfully_processed_date"
    TIMESTAMP_COL = "last_update_timestamp"

    def __init__(self, spark: SparkSession, db_properties: dict, table_name: str):
        """
        Initializes the tracker.

        Args:
            spark (SparkSession): The active SparkSession.
            db_properties (dict): Dictionary from _get_db_connection_properties
                                  containing 'url', 'user', 'password', 'driver', etc.
            table_name (str): The fully qualified name of the metadata table (e.g., 'metadata_db.job_tracker').
        """
        if not spark:
            raise ValueError("SparkSession is required for DatabaseMetadataTracker.")
        if not db_properties or not db_properties.get('url'):
            raise ValueError("Database connection properties (including url) are required.")
        if not table_name:
            raise ValueError("Database table name is required.")

        self.spark = spark
        self.db_properties = db_properties # Contient url, user, password, driver...
        self.table_name = table_name
        # Note: url doit être complet ici (incluant la base si nécessaire par le driver)
        self.jdbc_url = db_properties['url']

        logger.info(f"Initializing DatabaseMetadataTracker for table: {self.table_name} at {self.jdbc_url}")
        # Pourrait inclure une vérification de l'existence de la table/colonnes ici si souhaité

    def read_last_processed_date(self, job_identifier: str) -> date | None:
        """Reads the last successfully processed date for the given job identifier from the DB."""
        if not job_identifier:
            raise ValueError("job_identifier cannot be empty.")

        logger.info(f"Reading last processed date for job '{job_identifier}' from table {self.table_name}")
        try:
            # Lire seulement la ligne pour ce job_identifier
            query = f"""
                (SELECT {self.DATE_COL}
                 FROM {self.table_name}
                 WHERE {self.JOB_ID_COL} = '{job_identifier}'
                ) AS read_query
            """
            logger.debug(f"Executing read query: {query.strip()}")

            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=query,
                properties=self.db_properties
            )

            # Récupérer le résultat (devrait être 0 ou 1 ligne)
            result = df.first() # Collecte seulement la première ligne si elle existe

            if result is None:
                logger.info(f"No metadata record found for job '{job_identifier}'.")
                return None
            else:
                last_date = getattr(result, self.DATE_COL, None) # Accès sécurisé à la colonne
                if isinstance(last_date, date):
                    logger.info(f"Found last processed date for job '{job_identifier}': {last_date.isoformat()}")
                    return last_date
                else:
                    # Gérer le cas où la date est NULL dans la BDD
                    logger.warning(f"Metadata record found for job '{job_identifier}', but date column is NULL or not a date.")
                    return None

        except Exception as e:
            logger.error(f"Failed to read metadata for job '{job_identifier}' from {self.table_name}: {e}", exc_info=True)
            # Important: Ne pas retourner None aveuglément, car une erreur DB peut masquer un état réel.
            # Relever l'erreur permet à l'appelant (date_utils) de décider de la stratégie.
            # Cependant, pour simplifier l'intégration avec date_utils actuel qui attend None en cas d'échec,
            # nous allons logger et retourner None, mais c'est un point à reconsidérer pour la robustesse.
            logger.warning(f"Returning None for last processed date due to read error for job '{job_identifier}'.")
            return None # Compromis pour l'intégration actuelle

    def write_last_processed_date(self, job_identifier: str, process_date: date):
        """
        Writes/Updates the successfully processed date for the given job identifier in the DB.
        Uses a read-modify-write approach with Spark DataFrames (less efficient for single row).
        Assumes table exists.
        """
        if not job_identifier:
            raise ValueError("job_identifier cannot be empty.")
        if not isinstance(process_date, date):
            raise ValueError("process_date must be a valid datetime.date object.")

        logger.info(f"Updating last processed date for job '{job_identifier}' to {process_date.isoformat()} in table {self.table_name}")

        try:
            # 1. Lire toutes les métadonnées existantes (sauf pour le job courant)
            # Attention: Peut être coûteux si la table de métadonnées est grande!
            logger.debug(f"Reading existing metadata (excluding current job '{job_identifier}')...")
            filter_cond = f"{self.JOB_ID_COL} != '{job_identifier}'"
            try:
                 # Lire toutes les colonnes nécessaires pour réécrire
                 other_jobs_df = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table=self.table_name,
                    properties=self.db_properties
                 ).where(filter_cond).select(self.JOB_ID_COL, self.DATE_COL, self.TIMESTAMP_COL)
                 other_jobs_count = other_jobs_df.count()
                 logger.debug(f"Read {other_jobs_count} records for other jobs.")
            except Exception as read_err:
                 # Si la table n'existe pas encore, on ne peut pas lire. On continue en supposant
                 # que l'écriture créera la table ou la première ligne.
                 # Analyser l'erreur spécifique est nécessaire pour plus de robustesse.
                 logger.warning(f"Could not read existing metadata (table might not exist?): {read_err}. Proceeding with insert/overwrite.")
                 other_jobs_df = None # Marquer comme non lu


            # 2. Créer un DataFrame pour la nouvelle ligne/mise à jour
            current_time = datetime.now()
            new_row_data = [(job_identifier, process_date, current_time)]
            schema = StructType([
                StructField(self.JOB_ID_COL, StringType(), False),
                StructField(self.DATE_COL, DateType(), True),
                StructField(self.TIMESTAMP_COL, TimestampType(), True) # Assurez-vous que ce type correspond à la BDD
            ])
            new_row_df = self.spark.createDataFrame(new_row_data, schema)
            logger.debug("Created DataFrame for the new/updated metadata row.")

            # 3. Combiner l'ancien (filtré) et le nouveau
            if other_jobs_df is not None and not other_jobs_df.rdd.isEmpty():
                 # S'assurer que les schémas correspondent avant l'union (surtout les types)
                 # Spark >= 3.0 `unionByName` est plus sûr si les schémas sont complexes ou l'ordre diffère
                 final_df_to_write = other_jobs_df.unionByName(new_row_df) # Utiliser unionByName par sécurité
                 logger.debug("Combined existing metadata (filtered) with the new row.")
            else:
                 # Si pas d'autres données ou lecture échouée, écrire seulement la nouvelle ligne
                 final_df_to_write = new_row_df
                 logger.debug("No existing metadata found or read failed, preparing to write only the new row.")

            # 4. Écrire le DataFrame combiné en mode overwrite
            # Ceci remplace TOUT le contenu de la table par le contenu de final_df_to_write
            logger.info(f"Writing combined metadata back to {self.table_name} using mode 'overwrite'.")
            final_df_to_write.write.jdbc(
                url=self.jdbc_url,
                table=self.table_name,
                mode="overwrite", # ATTENTION: Efface la table et réécrit !
                properties=self.db_properties
            )
            logger.info(f"Successfully wrote metadata update for job '{job_identifier}'.")

        except Exception as e:
            logger.error(f"Failed to write metadata for job '{job_identifier}' to {self.table_name}: {e}", exc_info=True)
            raise # Relever l'erreur pour indiquer l'échec de l'écriture


# --- Fonctions Publiques Simplifiées ---
# Ces fonctions utilisent le factory pour obtenir le tracker et appeler ses méthodes
def read_last_processed_date(config):
    """
    Reads the last processed date for the job specified in the config.

    Args:
        config (dict): The application configuration containing 'metadata' section
                       and 'job_name_identifier'.

    Returns:
        datetime.date or None: The last processed date or None.
    """
    job_identifier = config.get("metadata", {}).get("job_name_identifier")
    if not job_identifier:
        raise ValueError("Configuration is missing 'job_name_identifier' within the 'metadata' section.")

    try:
        tracker = get_metadata_tracker(config)
        return tracker.read_last_processed_date(job_identifier)
    except (ValueError, NotImplementedError, IOError) as e:
         logger.error(f"Failed to read metadata for job '{job_identifier}': {e}")
         # Depending on policy, either return None or re-raise
         # Returning None might lead to processing yesterday on error, which might be safer
         # than failing completely or processing large amounts of data.
         logger.warning(f"Returning None for last processed date due to error for job '{job_identifier}'.")
         return None
    except Exception as e:
         logger.error(f"Unexpected error reading metadata for job '{job_identifier}': {e}", exc_info=True)
         logger.warning(f"Returning None for last processed date due to unexpected error for job '{job_identifier}'.")
         return None


def write_last_processed_date(config, process_date):
    """
    Writes the last processed date for the job specified in the config.

    Args:
        config (dict): The application configuration containing 'metadata' section
                       and 'job_name_identifier'.
        process_date (datetime.date): The date to write.
    """
    job_identifier = config.get("metadata", {}).get("job_name_identifier")
    if not job_identifier:
        raise ValueError("Configuration is missing 'job_name_identifier' within the 'metadata' section.")

    try:
        tracker = get_metadata_tracker(config)
        tracker.write_last_processed_date(job_identifier, process_date)
    except (ValueError, NotImplementedError, IOError) as e:
         # Log error, but allow job to potentially finish if processing was OK.
         # The calling function (main.py) handles the criticality.
         logger.error(f"Failed to write metadata for job '{job_identifier}' with date {process_date.isoformat()}: {e}", exc_info=True)
         # Re-raise the error to indicate the metadata write failed
         raise IOError(f"Failed to write metadata for job '{job_identifier}'") from e
    except Exception as e:
         logger.error(f"Unexpected error writing metadata for job '{job_identifier}': {e}", exc_info=True)
         raise IOError(f"Unexpected error writing metadata for job '{job_identifier}'") from e


# --- Example usage for testing ---
if __name__ == '__main__':
    print("Testing common metadata functions...")
    # Dummy config structure for testing file tracker
    test_dir = "./temp_common_metadata_test"
    test_file = os.path.join(test_dir, "common_metadata.json")
    job1_id = "job_alpha"
    job2_id = "job_beta"

    test_config_job1 = {
        "metadata": {
            "tracker_file_path": test_file,
            "job_name_identifier": job1_id
        }
        # Add other config sections if needed by get_metadata_tracker in future
    }
    test_config_job2 = {
        "metadata": {
            "tracker_file_path": test_file, # Same file
            "job_name_identifier": job2_id
        }
    }


    # Clean up previous test file
    if os.path.exists(test_file):
        os.remove(test_file)
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    print(f"\n--- Testing with file: {test_file} ---")

    # 1. Read non-existent file/job
    print("\n1. Reading non-existent job1:")
    date1 = read_last_processed_date(test_config_job1)
    print(f"Result job1: {date1}")
    assert date1 is None

    # 2. Write for job1
    print("\n2. Writing for job1:")
    today = date.today()
    try:
        write_last_processed_date(test_config_job1, today)
        print("Write successful.")
    except Exception as e:
        print(f"Write failed: {e}")

    # 3. Read for job1 (should exist) and job2 (should not exist)
    print("\n3. Reading job1 and job2:")
    date1 = read_last_processed_date(test_config_job1)
    date2 = read_last_processed_date(test_config_job2)
    print(f"Result job1: {date1}")
    print(f"Result job2: {date2}")
    assert date1 == today
    assert date2 is None

    # 4. Write for job2
    print("\n4. Writing for job2:")
    yesterday = today - timedelta(days=1) # Requires timedelta
    try:
        write_last_processed_date(test_config_job2, yesterday)
        print("Write successful.")
    except Exception as e:
        print(f"Write failed: {e}")

    # 5. Read both jobs again
    print("\n5. Reading job1 and job2 again:")
    date1 = read_last_processed_date(test_config_job1)
    date2 = read_last_processed_date(test_config_job2)
    print(f"Result job1: {date1}")
    print(f"Result job2: {date2}")
    assert date1 == today
    assert date2 == yesterday

    # 6. Verify file content
    print("\n6. Verifying file content:")
    if os.path.exists(test_file):
        with open(test_file, 'r') as f:
            content = json.load(f)
            print(json.dumps(content, indent=4))
            assert job1_id in content
            assert content[job1_id]["last_successfully_processed_date"] == today.isoformat()
            assert job2_id in content
            assert content[job2_id]["last_successfully_processed_date"] == yesterday.isoformat()
    else:
        print("Test file not found!")

    print("\nCommon metadata tests complete.")
    # Consider cleaning up test_dir and test_file