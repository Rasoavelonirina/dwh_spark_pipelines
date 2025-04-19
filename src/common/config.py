# src/common/config.py

import configparser
import yaml
import os
import logging
import warnings
from collections.abc import Mapping # Import Mapping for deep merge

logger = logging.getLogger(__name__)

# --- Security Warning ---
warnings.warn(
    "Loading configuration possibly containing plain text secrets from .ini files. "
    "This is insecure. Migrate to environment variables or a secret management system.",
    UserWarning
)
# --- End Security Warning ---

def _load_ini_file(file_path):
    """Loads an INI file and returns its content as a nested dictionary."""
    if not os.path.exists(file_path):
        logger.warning(f"INI configuration file not found: {file_path}")
        return {}

    parser = configparser.ConfigParser()
    try:
        # Preserve case for keys if needed (e.g., database names)
        # parser.optionxform = str # Uncomment if case sensitivity is crucial for INI keys
        parser.read(file_path)
        config_dict = {section: dict(parser.items(section)) for section in parser.sections()}
        logger.info(f"Successfully loaded INI config: {file_path}")
        # Basic check for passwords
        for section, items in config_dict.items():
            if 'pwd' in items or 'password' in items:
                 logger.warning(f"Plain text password found in section '[{section}]' of {file_path}. This is insecure!")
        return config_dict
    except configparser.Error as e:
        logger.error(f"Error parsing INI file {file_path}: {e}", exc_info=True)
        raise

def _load_yaml_file(file_path):
    """Loads a YAML file and returns its content as a dictionary."""
    if not os.path.exists(file_path):
        logger.warning(f"YAML configuration file not found: {file_path}")
        # Return empty dict instead of raising error, allows optional common YAMLs
        return {}

    try:
        with open(file_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        logger.info(f"Successfully loaded YAML config: {file_path}")
        return config_dict or {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {file_path}: {e}", exc_info=True)
        raise
    except IOError as e:
        logger.error(f"Error reading YAML file {file_path}: {e}", exc_info=True)
        raise

def _deep_merge_dicts(base_dict, merge_dict):
    """
    Recursively merges merge_dict into base_dict.
    Keys from merge_dict overwrite keys in base_dict.
    If both values are dictionaries, performs a deep merge.
    """
    if not isinstance(merge_dict, Mapping):
        return merge_dict # merge_dict is not a dict, overwrite base_dict

    result = dict(base_dict) # Make a copy

    for key, value in merge_dict.items():
        if key in result and isinstance(result[key], Mapping) and isinstance(value, Mapping):
            result[key] = _deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def load_app_config(job_config_path, common_config_dir="config/common"):
    """
    Loads the application configuration by merging common INI and YAML files
    and the job-specific YAML file.

    Loading Order & Precedence (Later steps overwrite earlier ones):
    1. Common INI files (db_connections.ini, remote_access.ini, etc.)
    2. Common YAML files (spark_defaults.yaml) -> Deep merged
    3. Job-specific YAML file (job_config_path) -> Deep merged

    Args:
        job_config_path (str): Path to the job-specific YAML config file.
        common_config_dir (str): Path to the directory containing common config files.

    Returns:
        dict: A dictionary containing the merged configuration.
    """
    logger.info(f"Loading application configuration...")
    final_config = {}

    # --- Step 1: Load common INI configurations ---
    if os.path.isdir(common_config_dir):
        for filename in sorted(os.listdir(common_config_dir)): # Sort for consistent load order
            if filename.endswith(".ini"):
                file_path = os.path.join(common_config_dir, filename)
                config_key = os.path.splitext(filename)[0] # Use filename without extension as key
                final_config[config_key] = _load_ini_file(file_path)
    else:
        logger.warning(f"Common configuration directory not found: {common_config_dir}")

    # --- Step 2: Load common YAML configurations (Deep Merge) ---
    if os.path.isdir(common_config_dir):
        for filename in sorted(os.listdir(common_config_dir)):
            if filename.endswith(".yaml") or filename.endswith(".yml"):
                file_path = os.path.join(common_config_dir, filename)
                common_yaml_config = _load_yaml_file(file_path)
                # Deep merge common YAML into final_config
                final_config = _deep_merge_dicts(final_config, common_yaml_config)
                logger.info(f"Deep merged common YAML: {filename}")

    # --- Step 3: Load job-specific YAML configuration (Deep Merge) ---
    if not os.path.exists(job_config_path):
        logger.error(f"Job-specific YAML configuration file not found: {job_config_path}")
        raise FileNotFoundError(f"Job-specific YAML configuration file not found: {job_config_path}")

    job_config = _load_yaml_file(job_config_path)
    # Deep merge job-specific config into the aggregated config
    final_config = _deep_merge_dicts(final_config, job_config)
    logger.info(f"Deep merged job-specific YAML: {os.path.basename(job_config_path)}")

    logger.info("Application configuration loaded successfully.")
    logger.debug(f"Final merged config keys: {list(final_config.keys())}")

    return final_config

# Example usage (for testing this module directly)
if __name__ == '__main__':
    # Assume structure from root of data_pipelines_project
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    common_dir = os.path.join(repo_root, 'config/common')
    job_config_file = os.path.join(repo_root, 'config/jobs/zebra_last_transaction.yaml') # Use the actual job config

    # --- Create dummy common files if they don't exist for testing ---
    if not os.path.exists(common_dir):
        os.makedirs(common_dir)
        print(f"Created dummy common config dir: {common_dir}")
    dummy_db_ini = os.path.join(common_dir, 'db_connections.ini')
    if not os.path.exists(dummy_db_ini):
         with open(dummy_db_ini, 'w') as f:
            f.write("[DM_v1_test]\nuri=test_uri\nuser=test_user\npwd=insecure\n") # Add dummy password for warning test
         print(f"Created dummy file: {dummy_db_ini}")
    dummy_spark_yaml = os.path.join(common_dir, 'spark_defaults.yaml')
    if not os.path.exists(dummy_spark_yaml):
        with open(dummy_spark_yaml, 'w') as f:
            f.write("spark_config:\n  spark.driver.memory: 512m\n  spark.executor.cores: 1\n")
        print(f"Created dummy file: {dummy_spark_yaml}")
    # --- End dummy file creation ---

    if not os.path.exists(job_config_file):
         print(f"Job config file not found: {job_config_file}. Cannot run test.")
    else:
         print("\nAttempting to load configuration...")
         try:
            loaded_config = load_app_config(job_config_file, common_dir)
            print("\n--- Loaded and Merged Configuration (Sample) ---")
            # print(loaded_config) # Avoid full print due to secrets

            print("\nAvailable Top-Level Keys:")
            print(list(loaded_config.keys()))

            print("\nSpark Config Section (Merged):")
            print(loaded_config.get('spark_config', 'Not Found'))

            print("\nDB Connections Section:")
            print(loaded_config.get('db_connections', 'Not Found'))

            print("\nJob Specific Database Reference:")
            print(f"database_connection_name: {loaded_config.get('database_connection_name')}")

            print("\nMetadata Section:")
            print(loaded_config.get('metadata', 'Not Found'))


         except Exception as e:
            print(f"\nError loading config during test: {e}")