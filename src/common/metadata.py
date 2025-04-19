# src/common/metadata.py

import json
import os
import logging
from datetime import date, datetime, timedelta
from abc import ABC, abstractmethod

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
    elif metadata_config.get("tracker_db_table"):
        tracker_type = "database" # Placeholder for future DB implementation
    else:
        raise ValueError("Metadata config must specify 'tracker_file_path' (for JSON) or 'tracker_db_table' (for DB - not implemented).")

    logger.info(f"Metadata tracker type determined as: {tracker_type}")

    if tracker_type == "json_file":
        file_path = metadata_config.get("tracker_file_path")
        if not file_path: # Should have been checked above, but double check
            raise ValueError("Missing 'tracker_file_path' for json_file metadata tracker.")
        return JsonFileMetadataTracker(file_path)

    elif tracker_type == "database":
        # --- Placeholder for Database Implementation ---
        logger.error("Database metadata tracker is not yet implemented.")
        raise NotImplementedError("Database metadata tracker is not implemented.")
        # db_connection_name = metadata_config.get("metadata_db_connection_name")
        # table_name = metadata_config.get("tracker_db_table")
        # if not db_connection_name or not table_name:
        #     raise ValueError("Database tracker requires 'metadata_db_connection_name' and 'tracker_db_table' in metadata config.")
        # # Need to get DB connection details from the main config using db_connection_name
        # db_config = config.get('db_connections', {}).get(db_connection_name)
        # if not db_config:
        #     raise ValueError(f"Database connection details not found for '{db_connection_name}' in 'db_connections' config.")
        # return DatabaseMetadataTracker(db_config, table_name) # Pass necessary details
        # --- End Placeholder ---

    else:
        # Should not be reachable if logic above is correct
        raise ValueError(f"Unsupported metadata tracker type: {tracker_type}")


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