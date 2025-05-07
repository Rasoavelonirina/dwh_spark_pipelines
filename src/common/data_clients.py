# src/common/data_clients.py
import logging, os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import stat
import shutil
import uuid
# Import helpers and handlers from other common modules
try:
    from common.base_data_handler import BaseDataHandler
    from common.jdbc_utils import JdbcHandler, get_db_connection_properties
    from common.mongo_utils import MongoHandler, get_mongo_connection_options
    from common.sftp_utils import SftpHandler, get_sftp_connection_details # Assuming sftp_utils exists
except ImportError as e:
     logging.error(f"Failed to import handler modules: {e}", exc_info=True)
     # Define dummies so subsequent calls fail clearly
     BaseDataHandler = None
     JdbcHandler = None
     MongoHandler = None
     SftpHandler = None
     get_db_connection_properties = None
     get_mongo_connection_options = None
     get_sftp_connection_details = None

logger = logging.getLogger(__name__)

def get_data_handler(
    spark: SparkSession,
    config: dict,
    logical_name: str, # Nom logique de la source ou sortie
    source_type: str = 'data_sources' # Indiquer si on cherche dans 'data_sources' ou 'outputs'
) -> BaseDataHandler | None:
    """
    Factory function to get a configured Data Handler instance (JDBC or Mongo).
    Reads base config from .ini files and applies overrides from job's .yaml file.

    Args:
        spark (SparkSession): The active SparkSession.
        config (dict): The application configuration.
        logical_name (str): The logical name of the data source or output (from job YAML).
        source_type (str): Key in the config where to find the logical name ('data_sources' or 'outputs').

    Returns:
        BaseDataHandler: An initialized handler instance.
        None: If configuration is missing, invalid, or handler cannot be created.
    """
    logger.info(f"Requesting Data Handler for logical '{source_type}' name: '{logical_name}'")
    if not BaseDataHandler or not JdbcHandler or not MongoHandler: # Vérif rapide imports
        logger.error("Handler classes failed to import. Cannot create handler.")
        return None

    try:
        # --- 1. Trouver la config pour le nom logique ---
        logical_config_section = config.get(source_type)
        if not logical_config_section or logical_name not in logical_config_section:
             raise ValueError(f"Logical name '{logical_name}' not found in '{source_type}' section of the job config.")
        logical_config = logical_config_section[logical_name]

        # --- 2. Extraire le nom physique et les overrides ---
        physical_connection_name = logical_config.get('connection_name')
        options_override = logical_config.get('options_override', {}) # Récupérer les overrides
        # db_type peut aussi être dans logical_config pour clarté, sinon on le prendra de la connexion physique
        db_type_logical = logical_config.get('db_type')

        if not physical_connection_name:
            # Gérer le cas des sorties fichiers qui n'ont pas de connection_name ?
            # Pour l'instant, on suppose que toutes les sources/sorties DB/Mongo ont une connexion physique.
            # Pour les fichiers, on pourrait retourner un autre type de handler ou None ici.
             if logical_config.get('output_type') == 'parquet_file':
                  logger.warning(f"'{logical_name}' seems to be a file output. No DB/Mongo handler created.")
                  # Le writer devra gérer ce cas différemment (peut-être pas besoin de handler)
                  return None # Pas de handler BDD/Mongo pour les fichiers
             else:
                  raise ValueError(f"Missing 'connection_name' (physical connection ref) for logical source/output '{logical_name}'.")


        # --- 3. Récupérer la config de la connexion physique ---
        db_connections = config.get('db_connections')
        if not db_connections or physical_connection_name not in db_connections:
            raise ValueError(f"Physical connection details for '{physical_connection_name}' (referenced by '{logical_name}') not found in 'db_connections' config.")
        conn_details = db_connections[physical_connection_name]

        # --- 4. Déterminer db_type (priorité à la config logique si définie) ---
        db_type = db_type_logical if db_type_logical else conn_details.get('db_type', 'jdbc')
        logger.info(f"Determined db_type as '{db_type}' for physical connection '{physical_connection_name}'.")

        # --- 5. Appeler la fonction helper appropriée AVEC les overrides ---
        if db_type.lower() in ['mariadb', 'mysql', 'postgres', 'sqlserver', 'jdbc']:
            if not get_db_connection_properties: raise ImportError("JDBC helper not loaded.")
            # Passer config, nom physique ET les overrides
            jdbc_url, properties = get_db_connection_properties(config, physical_connection_name, options_override)
            return JdbcHandler(spark, logical_name, jdbc_url, properties) # Initialiser avec nom logique

        elif db_type.lower() == 'mongodb':
            if not get_mongo_connection_options: raise ImportError("Mongo helper not loaded.")
            # Passer config, nom physique ET les overrides
            mongo_options = get_mongo_connection_options(config, physical_connection_name, options_override)
            return MongoHandler(spark, logical_name, mongo_options) # Initialiser avec nom logique

        else:
            raise ValueError(f"Unsupported db_type '{db_type}' found for connection '{physical_connection_name}'.")

    except (ValueError, ImportError) as e:
        logger.error(f"Failed to configure Data Handler for '{logical_name}': {e}", exc_info=True)
        return None
    except Exception as e:
         logger.error(f"Unexpected error creating Data Handler for '{logical_name}': {e}", exc_info=True)
         return None


def get_sftp_handler(config: dict, logical_name: str, source_type: str = 'data_sources') -> SftpHandler | None:
    """
    Factory function to get a configured SftpHandler instance.

    Args:
        config (dict): The application configuration.
        connection_name (str): The name of the SFTP connection (from remote_access.ini).

    Returns:
        SftpHandler: An initialized SftpHandler instance ready for use with 'with'.
        None: If configuration is missing or invalid.
    """
    logger.info(f"Requesting Sftp Handler for logical '{source_type}' name: '{logical_name}'")
    if not SftpHandler or not get_sftp_connection_details:
         logger.error("SftpHandler class or helper failed to import.")
         return None

    try:
        logical_config_section = config.get(source_type)
        if not logical_config_section or logical_name not in logical_config_section:
             raise ValueError(f"Logical name '{logical_name}' not found in '{source_type}' section.")
        logical_config = logical_config_section[logical_name]

        physical_connection_name = logical_config.get('connection_name')
        options_override = logical_config.get('options_override', {}) # Récupérer overrides

        if not physical_connection_name:
             # Peut-être gérer un type 'local_file' ici si nécessaire
             raise ValueError(f"Missing 'connection_name' for SFTP source/output '{logical_name}'.")

        # Utiliser la fonction helper pour obtenir les arguments fusionnés
        connection_args = get_sftp_connection_details(config, physical_connection_name, options_override)

        # Instancier SftpHandler avec les arguments préparés
        return SftpHandler(**connection_args)

    except ValueError as e:
        logger.error(f"Failed to configure SftpHandler for '{logical_name}': {e}", exc_info=True)
        return None
    
def read_data(spark: SparkSession, config: dict, logical_source_name: str, **kwargs) -> DataFrame:
    """
    Reads data from a configured logical source, supporting various types and options.

    Determines the source type and method based on the configuration under
    'data_sources[logical_source_name]' and optional kwargs.

    Args:
        spark (SparkSession): The active SparkSession.
        config (dict): The application configuration dictionary.
        logical_source_name (str): The logical name of the data source defined
                                   in the job's YAML file under 'data_sources'.
        **kwargs: Optional keyword arguments to control specific read behaviors:
            - query_string (str): For JDBC, the SQL query to execute (provide query_alias too).
            - query_alias (str): For JDBC, the alias required by Spark when using query_string.
            - pipeline (list | str): For MongoDB, the aggregation pipeline to execute.
            - schema (StructType | str): For file-based sources (CSV, JSON), the schema to apply.
            - file_pattern (str): For file/SFTP sources, a specific file pattern (e.g., "*.csv").
            - remote_path (str): For SFTP, the specific remote file/directory path.
            - local_temp_dir (str): For SFTP, path to download temporary files.
            - Any other option accepted by the underlying Spark reader (e.g., header=True for CSV).

    Returns:
        DataFrame: The resulting Spark DataFrame.

    Raises:
        ValueError: If configuration is missing or invalid for the source type.
        TypeError: If the handler type doesn't match the requested operation (e.g., query on Mongo).
        NotImplementedError: If a requested source type or operation is not yet supported.
        Exception: Any underlying Spark or I/O exception during read.
    """
    logger.info(f"Attempting to read data for logical source: '{logical_source_name}'")

    # --- 1. Get Logical Source Configuration ---
    source_configs = config.get("data_sources")
    if not source_configs or logical_source_name not in source_configs:
        raise ValueError(f"Configuration for logical source '{logical_source_name}' not found in 'data_sources' section.")
    source_config = source_configs[logical_source_name]

    # --- 2. Determine Source Type ---
    # Priorité à une clé explicite 'source_type', sinon essayer de deviner
    source_type = source_config.get('source_type')
    if not source_type:
        # Tentative de déduction (peut nécessiter ajustement)
        if source_config.get('db_type') == 'mongodb': source_type = 'mongo_collection'
        elif source_config.get('db_type') in ['mariadb', 'mysql', 'postgres', 'jdbc']: source_type = 'jdbc_table'
        elif source_config.get('connection_name') and 'sftp' in source_config.get('connection_name').lower(): source_type = 'sftp_file' # Heuristique simple
        elif source_config.get('path'): source_type = source_config.get('format', 'parquet_file') # Deviner format si path existe
        else: raise ValueError(f"Cannot determine 'source_type' for '{logical_source_name}'. Specify explicitly.")
    source_type = source_type.lower()
    logger.info(f"Determined source type as: '{source_type}' for logical source '{logical_source_name}'.")

    # --- 3. Prepare Base Read Options (from config override) ---
    base_options = source_config.get('options_override', {})
    # Fusionner avec les options passées en kwargs (kwargs ont priorité)
    final_read_options = {**base_options, **kwargs}
    logger.debug(f"Base read options (after override merge): {final_read_options}")


    # --- 4. Dispatch Read Operation based on Source Type ---

    # --- JDBC ---
    if source_type in ['jdbc_table', 'mariadb_table', 'mysql_table', 'postgres_table']:
        target_table = source_config.get('table')
        query_string = final_read_options.pop('query_string', None) # Extraire et retirer de options
        query_alias = final_read_options.pop('query_alias', f"{logical_source_name}_q") # Extraire et retirer

        if not query_string and not target_table:
             raise ValueError(f"Missing 'table' (for full read) or 'query_string'/'query_alias' (for query read) in config/args for JDBC source '{logical_source_name}'.")

        handler = get_data_handler(spark, config, logical_source_name, source_type='data_sources')
        if not handler or not isinstance(handler, JdbcHandler):
            raise TypeError(f"Could not get valid JdbcHandler for '{logical_source_name}'.")

        if query_string:
             logger.info(f"Performing JDBC query read for '{logical_source_name}'.")
             return handler.read_query(query_alias=query_alias, query_string=query_string, options=final_read_options)
        else:
             logger.info(f"Performing JDBC table read for '{logical_source_name}'.")
             return handler.read(target=target_table, options=final_read_options)

    # --- MongoDB ---
    elif source_type == 'mongo_collection':
        target_collection = source_config.get('collection')
        pipeline = final_read_options.pop('pipeline', None) # Extraire et retirer
        schema = final_read_options.pop('schema', None) # Extraire et retirer schema si fourni

        if not target_collection:
            raise ValueError(f"Missing 'collection' in config for MongoDB source '{logical_source_name}'.")

        handler = get_data_handler(spark, config, logical_source_name, source_type='data_sources')
        if not handler or not isinstance(handler, MongoHandler):
            raise TypeError(f"Could not get valid MongoHandler for '{logical_source_name}'.")

        # Ajouter le schéma aux options s'il est fourni
        if schema: final_read_options['schema'] = schema

        if pipeline:
            logger.info(f"Performing MongoDB pipeline read for '{logical_source_name}'.")
            return handler.read_pipeline(target=target_collection, pipeline=pipeline, options=final_read_options)
        else:
            logger.info(f"Performing MongoDB collection read for '{logical_source_name}'.")
            return handler.read(target=target_collection, options=final_read_options)

    # --- File Systems (Parquet, CSV, JSON...) ---
    elif source_type in ['parquet_file', 'csv_file', 'json_file', 'orc_file']:
        file_format = source_type.split('_')[0] # Extraire 'parquet', 'csv', etc.
        path = source_config.get('path')
        file_pattern = final_read_options.pop('file_pattern', None) # Ex: "*.parquet"

        if not path: raise ValueError(f"Missing 'path' in config for file source '{logical_source_name}'.")

        # Construire le chemin final (potentiellement avec pattern)
        final_path = os.path.join(path, file_pattern) if file_pattern else path
        logger.info(f"Reading {file_format} file(s) from path: {final_path}")

        schema = final_read_options.pop('schema', None) # Schéma optionnel pour CSV/JSON
        # Pop other known file options to avoid passing them all blindly
        header = final_read_options.pop('header', None)
        infer_schema = final_read_options.pop('inferSchema', None)
        sep = final_read_options.pop('sep', None)
        encoding = final_read_options.pop('encoding', None)
        multiline = final_read_options.pop('multiLine', None) # Pour JSON

        try:
            reader = spark.read.format(file_format)
            # Appliquer les options connues
            if header is not None and file_format == 'csv': reader = reader.option("header", str(header).lower())
            if infer_schema is not None: reader = reader.option("inferSchema", str(infer_schema).lower())
            if sep is not None and file_format == 'csv': reader = reader.option("sep", sep)
            if encoding is not None: reader = reader.option("encoding", encoding)
            if multiline is not None and file_format == 'json': reader = reader.option("multiLine", str(multiline).lower())

            # Appliquer le schéma s'il est fourni
            if schema:
                 if isinstance(schema, StructType): reader = reader.schema(schema)
                 elif isinstance(schema, str): reader = reader.schema(schema) # DDL string format
                 else: logger.warning("Schema provided but is not StructType or string, ignoring.")
            elif file_format == 'csv' and str(infer_schema).lower() != 'true':
                 logger.warning(f"Reading CSV '{logical_source_name}' without schema or inferSchema=true. Columns may be string.")
            elif file_format == 'json' and str(infer_schema).lower() != 'true':
                 logger.warning(f"Reading JSON '{logical_source_name}' without schema or inferSchema=true. Schema might be incomplete.")


            # Appliquer les options restantes (override ou spécifiques au format)
            reader = reader.options(**final_read_options)

            return reader.load(final_path)
        except Exception as e:
             logger.error(f"Failed to read {file_format} from {final_path}: {e}", exc_info=True)
             raise

    # --- SFTP File ---
    elif source_type in ['sftp_file', 'sftp_csv', 'sftp_parquet']: # Être plus spécifique sur le format attendu
        file_format = source_type.split('_')[-1] # csv, parquet, etc.
        if file_format == 'file': file_format = 'text' # ou binaire?

        # Extraire les args spécifiques à SFTP de kwargs ou config?
        remote_path = final_read_options.pop('remote_path', source_config.get('remote_path')) # Chemin sur le serveur SFTP
        file_pattern = final_read_options.pop('file_pattern', None) # Pattern pour filtrer les fichiers
        local_temp_dir = final_read_options.pop('local_temp_dir', "/tmp/spark_sftp_downloads") # Où télécharger

        if not remote_path: raise ValueError(f"Missing 'remote_path' in args or config for SFTP source '{logical_source_name}'.")
        if not SftpHandler: raise ImportError("SftpHandler not available.")

        sftp_handler = get_sftp_handler(config, logical_source_name, source_type='data_sources')
        if not sftp_handler:
             raise RuntimeError(f"Failed to get SftpHandler for '{logical_source_name}'.")

        downloaded_files_paths = []
        try:
            with sftp_handler as sftp:
                logger.info(f"Accessing SFTP source '{logical_source_name}' at path '{remote_path}'.")
                # Construire le chemin local temporaire unique
                import uuid
                run_temp_dir = os.path.join(local_temp_dir, f"{logical_source_name}_{uuid.uuid4()}")
                os.makedirs(run_temp_dir, exist_ok=True)
                logger.debug(f"Using temporary download directory: {run_temp_dir}")

                # Lister les fichiers correspondant au pattern (si fourni)
                files_to_download = []
                if sftp.isdir(remote_path):
                     all_files = sftp.list_dir_attr(remote_path)
                     if file_pattern:
                          import fnmatch
                          files_to_download = [
                               f.filename for f in all_files
                               if stat.S_ISREG(f.st_mode) and fnmatch.fnmatch(f.filename, file_pattern)
                          ]
                     else: # Prendre tous les fichiers si pas de pattern
                          files_to_download = [f.filename for f in all_files if stat.S_ISREG(f.st_mode)]
                     remote_base_path = remote_path
                elif sftp.isfile(remote_path):
                     # Si le chemin est un fichier unique, le télécharger (ignorer pattern?)
                     files_to_download = [os.path.basename(remote_path)]
                     remote_base_path = os.path.dirname(remote_path)
                else:
                     raise FileNotFoundError(f"Remote path '{remote_path}' not found or not accessible on SFTP server.")

                if not files_to_download:
                     logger.warning(f"No files found matching pattern '{file_pattern if file_pattern else '*'}' at '{remote_path}'. Returning empty DataFrame.")
                     # Retourner un DF vide avec le schéma si possible? Difficile sans lire...
                     # Pour l'instant, levons une exception contrôlée ou retournons None
                     # raise FileNotFoundError("No matching SFTP files found.")
                     # OU Retourner un DF vide:
                     logger.warning("Returning empty DataFrame as no SFTP files were found/downloaded.")
                     # Essayer de créer un DF vide avec le schéma passé en argument, sinon sans schéma
                     schema_arg = kwargs.get('schema', final_read_options.get('schema'))
                     if schema_arg:
                         return spark.createDataFrame([], schema_arg)
                     else:
                         # Retourne un DF complètement vide, l'appelant devra gérer
                         return spark.createDataFrame([], StructType([]))


                # Télécharger les fichiers
                for fname in files_to_download:
                    remote_file = f"{remote_base_path}/{fname}" # Construire chemin complet si besoin
                    local_file = os.path.join(run_temp_dir, fname)
                    sftp.download_file(remote_file, local_file)
                    downloaded_files_paths.append(local_file)

            # Lire les fichiers téléchargés avec Spark
            if downloaded_files_paths:
                 read_path = run_temp_dir # Lire tout le répertoire temporaire
                 logger.info(f"Reading {len(downloaded_files_paths)} downloaded file(s) of format '{file_format}' from {read_path}")

                 # Utiliser la même logique de lecture que pour les fichiers locaux
                 reader = spark.read.format(file_format)
                 # Appliquer options connues (header, sep, schema, etc.) depuis final_read_options
                 header = final_read_options.pop('header', None)
                 infer_schema = final_read_options.pop('inferSchema', None)
                 sep = final_read_options.pop('sep', None)
                 schema_arg = kwargs.get('schema', final_read_options.pop('schema', None))
                 # ... autres options ...
                 if header is not None and file_format == 'csv': reader = reader.option("header", str(header).lower())
                 if infer_schema is not None: reader = reader.option("inferSchema", str(infer_schema).lower())
                 if sep is not None and file_format == 'csv': reader = reader.option("sep", sep)
                 if schema_arg:
                     if isinstance(schema_arg, StructType): reader = reader.schema(schema_arg)
                     elif isinstance(schema_arg, str): reader = reader.schema(schema_arg)
                 # ...
                 reader = reader.options(**final_read_options) # Appliquer reste des options

                 return reader.load(read_path) # Spark peut lire un dir contenant les fichiers
            else:
                 # Ce cas est déjà géré ci-dessus (retour DF vide)
                 pass

        except Exception as e:
             logger.error(f"Failed during SFTP download or subsequent read for '{logical_source_name}': {e}", exc_info=True)
             raise
        finally:
            # Nettoyer les fichiers temporaires (optionnel, peut être utile pour debug)
            if downloaded_files_paths and run_temp_dir and os.path.exists(run_temp_dir):
                 try:
                      logger.debug(f"Cleaning up temporary SFTP download directory: {run_temp_dir}")
                      shutil.rmtree(run_temp_dir)
                 except Exception as cleanup_e:
                      logger.warning(f"Could not cleanup temporary directory {run_temp_dir}: {cleanup_e}")

    # --- Type de Source Inconnu ---
    else:
        raise NotImplementedError(f"Reading from source_type '{source_type}' is not implemented yet for logical source '{logical_source_name}'.")

# --- NOUVELLE Fonction write_data ---
def write_data(df: DataFrame, spark: SparkSession, config: dict, logical_output_name: str, **kwargs):
    """
    Writes a DataFrame to a configured logical output destination.
    Supports various types (JDBC, Mongo, Parquet, CSV, SFTP...) and options.

    Args:
        df (DataFrame): The Spark DataFrame to write.
        spark (SparkSession): The active SparkSession.
        config (dict): The application configuration dictionary.
        logical_output_name (str): The logical name of the output destination defined
                                   in the job's YAML file under 'outputs'.
        **kwargs: Optional keyword arguments to override configuration or pass specific
                  write options not suitable for YAML (rare for writes).
                  Example: write_mode='append' (overrides YAML mode).

    Raises:
        ValueError: If configuration is missing or invalid for the output type.
        TypeError: If the handler type doesn't match the requested operation.
        NotImplementedError: If a requested output type or operation is not yet supported.
        Exception: Any underlying Spark or I/O exception during write.
    """
    logger.info(f"Attempting to write data for logical output: '{logical_output_name}'")

    if df is None or df.rdd.isEmpty():
        logger.warning(f"Input DataFrame for output '{logical_output_name}' is None or empty. Nothing to write.")
        return # Exit gracefully if nothing to write

    # --- 1. Get Logical Output Configuration ---
    output_configs = config.get("outputs")
    if not output_configs or logical_output_name not in output_configs:
        raise ValueError(f"Configuration for logical output '{logical_output_name}' not found in 'outputs' section.")
    output_config = output_configs[logical_output_name]

    # --- 2. Determine Output Type ---
    output_type = output_config.get('output_type')
    if not output_type:
        # Essayer de deviner basé sur la présence de clés (moins fiable pour output)
        if output_config.get('table') and output_config.get('connection_name'): output_type = 'jdbc_table'
        elif output_config.get('collection') and output_config.get('connection_name'): output_type = 'mongo_collection'
        elif output_config.get('path') and not output_config.get('connection_name'): output_type = 'parquet_file' # Default to parquet
        elif output_config.get('connection_name') and 'sftp' in output_config.get('connection_name','').lower(): output_type = 'sftp_file'
        else: raise ValueError(f"Cannot determine 'output_type' for '{logical_output_name}'. Specify explicitly.")
    output_type = output_type.lower()
    logger.info(f"Determined output type as: '{output_type}' for logical output '{logical_output_name}'.")

    # --- 3. Prepare Base Write Options and Mode ---
    base_options = output_config.get('options_override', {})
    final_write_options = {**base_options, **kwargs} # kwargs peuvent override options_override
    # Mode d'écriture: priorité kwargs > YAML > défaut 'overwrite'
    write_mode = final_write_options.pop('mode', output_config.get('mode', 'overwrite'))
    if write_mode not in ["append", "overwrite", "ignore", "error", "errorifexists"]:
        logger.warning(f"Invalid write mode '{write_mode}' provided. Defaulting to 'overwrite'.")
        write_mode = 'overwrite'
    logger.info(f"Using write mode: '{write_mode}'")
    logger.debug(f"Base write options (after override merge): {final_write_options}")


    # --- 4. Dispatch Write Operation based on Output Type ---

    # --- JDBC ---
    if output_type in ['jdbc_table', 'mariadb_table', 'mysql_table', 'postgres_table']:
        target_table = output_config.get('table')
        if not target_table: raise ValueError(f"Missing 'table' for JDBC output '{logical_output_name}'.")

        handler = get_data_handler(spark, config, logical_output_name, source_type='outputs')
        if not handler or not isinstance(handler, JdbcHandler):
            raise TypeError(f"Could not get valid JdbcHandler for output '{logical_output_name}'.")

        logger.info(f"Performing JDBC table write to '{target_table}'.")
        try:
            # Passer les options fusionnées à la méthode write du handler
            handler.write(df, target=target_table, mode=write_mode, options=final_write_options)
        except Exception as e:
             # L'erreur est déjà loguée dans le handler, on la propage
             raise e

    # --- MongoDB ---
    elif output_type == 'mongo_collection':
        target_collection = output_config.get('collection')
        if not target_collection: raise ValueError(f"Missing 'collection' for MongoDB output '{logical_output_name}'.")

        handler = get_data_handler(spark, config, logical_output_name, source_type='outputs')
        if not handler or not isinstance(handler, MongoHandler):
            raise TypeError(f"Could not get valid MongoHandler for output '{logical_output_name}'.")

        logger.info(f"Performing MongoDB collection write to '{target_collection}'.")
        try:
            # Passer les options fusionnées à la méthode write du handler
            handler.write(df, target=target_collection, mode=write_mode, options=final_write_options)
        except Exception as e:
             raise e

    # --- File Systems (Parquet, CSV, JSON...) ---
    elif output_type in ['parquet_file', 'csv_file', 'json_file', 'orc_file', 'text_file']:
        file_format = output_type.split('_')[0] # parquet, csv, json, orc, text
        path = output_config.get('path')
        partition_columns = output_config.get('partition_by') # Doit être une liste

        if not path: raise ValueError(f"Missing 'path' for file output '{logical_output_name}'.")

        logger.info(f"Writing {file_format} file(s) to path: {path}")
        if partition_columns: logger.info(f"Partitioning by: {partition_columns}")

        # Pop options connues pour les appliquer spécifiquement
        header = final_write_options.pop('header', None)
        sep = final_write_options.pop('sep', None)
        encoding = final_write_options.pop('encoding', None)
        compression = final_write_options.pop('compression', None)
        # Ajouter d'autres options de fichier si besoin

        try:
            writer = df.write.format(file_format).mode(write_mode)
            if partition_columns and isinstance(partition_columns, list) and partition_columns:
                writer = writer.partitionBy(*partition_columns)

            # Appliquer options connues
            if header is not None and file_format == 'csv': writer = writer.option("header", str(header).lower())
            if sep is not None and file_format == 'csv': writer = writer.option("sep", sep)
            if encoding is not None: writer = writer.option("encoding", encoding)
            if compression is not None: writer = writer.option("compression", compression)

            # Appliquer les options restantes (override ou spécifiques au format)
            writer = writer.options(**final_write_options)

            writer.save(path)
            logger.info(f"Successfully wrote {file_format} data to: {path}")

        except Exception as e:
             logger.error(f"Failed to write {file_format} data to {path}: {e}", exc_info=True)
             raise

    # --- SFTP File ---
    elif output_type in ['sftp_file', 'sftp_csv', 'sftp_parquet']: # Être spécifique
        file_format_to_write = output_type.split('_')[-1] # csv, parquet, etc.
        if file_format_to_write == 'file': file_format_to_write = 'text' # Ou gérer binaire?

        remote_base_path = output_config.get('remote_base_path') # Où uploader sur le serveur SFTP
        # Nom de fichier pourrait être dynamique (basé sur date, etc.) passé en kwargs?
        output_filename_pattern = final_write_options.pop('output_filename_pattern', f"{logical_output_name}_output") # Ex: "report_{date}.csv"
        local_temp_dir = final_write_options.pop('local_temp_dir', "/tmp/spark_sftp_uploads")

        if not remote_base_path: raise ValueError(f"Missing 'remote_base_path' for SFTP output '{logical_output_name}'.")
        if not SftpHandler: raise ImportError("SftpHandler not available.")

        sftp_handler = get_sftp_handler(config, logical_output_name, source_type='outputs')
        if not sftp_handler:
             raise RuntimeError(f"Failed to get SftpHandler for output '{logical_output_name}'.")

        # --- Écrire d'abord dans un répertoire temporaire LOCAL ---
        # Créer un nom de fichier/dossier temporaire unique localement
        run_temp_dir = os.path.join(local_temp_dir, f"{logical_output_name}_{uuid.uuid4()}")
        local_write_path = os.path.join(run_temp_dir, "data") # Écrire dans un sous-dossier "data"

        logger.info(f"Writing intermediate '{file_format_to_write}' files to local temp path: {local_write_path}")
        # --- Utiliser la logique d'écriture fichier standard pour écrire localement ---
        try:
            # Repartitionner AVANT l'écriture locale pour contrôler le nombre de fichiers à uploader ?
            # df_to_write_local = df.repartition(1) # Écrire 1 fichier (peut être gros)
            df_to_write_local = df # Ou garder le partitionnement existant

            # Extraire les options spécifiques au format pour l'écriture locale
            local_write_options = final_write_options.copy() # Travailler sur une copie
            header = local_write_options.pop('header', None)
            sep = local_write_options.pop('sep', None)
            encoding = local_write_options.pop('encoding', None)
            compression = local_write_options.pop('compression', None) # Important pour Parquet/ORC

            local_writer = df_to_write_local.write.format(file_format_to_write).mode("overwrite") # Toujours overwrite dans temp
            if header is not None and file_format_to_write == 'csv': local_writer = local_writer.option("header", str(header).lower())
            if sep is not None and file_format_to_write == 'csv': local_writer = local_writer.option("sep", sep)
            if encoding is not None: local_writer = local_writer.option("encoding", encoding)
            if compression is not None: local_writer = local_writer.option("compression", compression)
            local_writer = local_writer.options(**local_write_options)

            local_writer.save(local_write_path)
            logger.info(f"Successfully wrote intermediate files to: {local_write_path}")

        except Exception as e:
             logger.error(f"Failed to write intermediate files to {local_write_path} for SFTP upload: {e}", exc_info=True)
             # Nettoyer si possible avant de lever l'erreur
             if os.path.exists(run_temp_dir): shutil.rmtree(run_temp_dir, ignore_errors=True)
             raise e

        # --- Uploader les fichiers écrits depuis le temp local vers SFTP ---
        files_uploaded_count = 0
        try:
            with sftp_handler as sftp:
                logger.info(f"Uploading files from {local_write_path} to SFTP remote path '{remote_base_path}'")
                sftp.make_dir(remote_base_path) # Assurer que le répertoire distant existe

                # Lister les fichiers réellement écrits par Spark (part-*)
                for filename in os.listdir(local_write_path):
                    if filename.startswith("part-") and not filename.endswith(".crc"): # Ignorer fichiers crc
                         local_file_path = os.path.join(local_write_path, filename)
                         # Construire nom de fichier distant (ex: renommer part-* en qqch de plus propre)
                         # Ici, on utilise un pattern simple. Pourrait être plus complexe (inclure date, etc.)
                         remote_filename = f"{output_filename_pattern}{filename.replace('part-', '_part_')}" # Ex: my_output_part_00000.parquet
                         remote_final_path = os.path.join(remote_base_path, remote_filename) # Utiliser os.path.join

                         logger.debug(f"Uploading '{local_file_path}' to '{remote_final_path}'")
                         sftp.upload_file(local_file_path, remote_final_path)
                         files_uploaded_count += 1

            logger.info(f"Successfully uploaded {files_uploaded_count} file(s) to SFTP remote path: {remote_base_path}")

        except Exception as e:
             logger.error(f"Failed during SFTP upload for '{logical_output_name}': {e}", exc_info=True)
             raise # Lever l'erreur si l'upload échoue
        finally:
             # Nettoyer le répertoire temporaire local
             if os.path.exists(run_temp_dir):
                  try:
                       logger.debug(f"Cleaning up temporary SFTP upload directory: {run_temp_dir}")
                       shutil.rmtree(run_temp_dir)
                  except Exception as cleanup_e:
                       logger.warning(f"Could not cleanup temporary directory {run_temp_dir}: {cleanup_e}")

    # --- Type de Sortie Inconnu ---
    else:
        raise NotImplementedError(f"Writing to output_type '{output_type}' is not implemented yet.")

    logger.info(f"--- Data Writing Finished for output '{logical_output_name}' ---")
