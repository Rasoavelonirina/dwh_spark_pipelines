# src/common/sftp_utils.py

import logging
import pysftp
import os
import stat # To check file types in listdir

logger = logging.getLogger(__name__)

# --- Configuration pour Pysftp (peut être ajustée) ---
# Ignorer les clés d'hôte (DANGEREUX pour la production, OK pour tests initiaux)
# CNOPTS = pysftp.CnOpts()
# CNOPTS.hostkeys = None
# OU spécifier le chemin vers votre fichier known_hosts
KNOWN_HOSTS_PATH = '~/.ssh/known_hosts' # Ou un chemin spécifique
CNOPTS = pysftp.CnOpts(knownhosts=KNOWN_HOSTS_PATH)

# Clés de base non overrideables
SFTP_CORE_CONNECTION_KEYS = {'uri', 'host', 'port', 'user', 'username', 'pwd', 'password', 'private_key'}


# --- Fonction pour obtenir les détails de connexion SFTP ---
def get_sftp_connection_details(
    config: dict,
    connection_name: str, # Nom physique de la connexion (ex: partner_report_sftp)
    overrides: dict = None # Options depuis le YAML du job
) -> dict:
    """
    Extracts SFTP details from .ini, merges overrides, prepares args for SftpHandler.
    """
    if not connection_name:
        raise ValueError("SFTP connection name cannot be empty.")
    if overrides is None:
        overrides = {}

    remote_access_config = config.get('remote_access')
    if not remote_access_config or connection_name not in remote_access_config:
        raise ValueError(f"SFTP connection details for '{connection_name}' not found in 'remote_access'.")
    conn_details = remote_access_config[connection_name]

    # --- Extraire détails de base depuis .ini ---
    host = conn_details.get('uri') # 'uri' contient le host
    port = int(conn_details.get('port', 22))
    user = conn_details.get('user')
    password = conn_details.get('pwd')
    private_key = conn_details.get('private_key') # Chemin vers clé privée
    # Options comportementales par défaut depuis .ini
    default_pk_pass = conn_details.get('private_key_pass')
    default_ciphers = conn_details.get('ciphers') # Sera une chaîne si défini
    default_log = conn_details.get('log', False) # pysftp prend bool ou chemin str
    # Ajouter d'autres options par défaut ici...

    # --- Validation de base ---
    if not host or not user: raise ValueError(f"Missing 'uri' or 'user' for SFTP '{connection_name}'.")
    if not password and not private_key: raise ValueError(f"Need 'pwd' or 'private_key' for SFTP '{connection_name}'.")

    # --- Préparer les arguments finaux pour pysftp.Connection ---
    final_args = {
        "host": host,
        "port": port,
        "username": user,
        "password": password, # Commencer avec les valeurs INI
        "private_key": private_key,
        "private_key_pass": default_pk_pass,
        "ciphers": None, # Sera traité ci-dessous
        "log": default_log,
        # Ajouter d'autres clés ici si pysftp.Connection les supporte
        #"cnopts": DEFAULT_CNOPTS # Utiliser les CnOpts par défaut (pour l'instant)
    }

    # Traiter les chiffrements par défaut (si chaîne, convertir en liste)
    if default_ciphers and isinstance(default_ciphers, str):
         final_args["ciphers"] = [c.strip() for c in default_ciphers.split(',') if c.strip()]
         logger.debug(f"Default ciphers for '{connection_name}': {final_args['ciphers']}")

    # Traiter la valeur log par défaut (convertir 'true'/'false' string en bool)
    if isinstance(default_log, str):
         if default_log.lower() == 'true': final_args["log"] = True
         elif default_log.lower() == 'false': final_args["log"] = False
         else: final_args["log"] = default_log # Assumer que c'est un chemin de fichier

    # --- Appliquer les Overrides (depuis YAML job) ---
    applied_overrides = {}
    if overrides and isinstance(overrides, dict):
        for key, value in overrides.items():
            if key.lower() in SFTP_CORE_CONNECTION_KEYS:
                logger.warning(f"Attempted to override core SFTP key '{key}' for '{connection_name}'. Override ignored.")
            elif key in final_args: # Si c'est une option connue que nous gérons
                # Traitement spécial pour ciphers (doit être une liste)
                if key == "ciphers" and isinstance(value, str):
                    final_args[key] = [c.strip() for c in value.split(',') if c.strip()]
                # Traitement spécial pour log (bool ou str)
                elif key == "log":
                     if isinstance(value, str):
                          if value.lower() == 'true': final_args[key] = True
                          elif value.lower() == 'false': final_args[key] = False
                          else: final_args[key] = value # Chemin fichier
                     elif isinstance(value, bool):
                           final_args[key] = value
                     else:
                          logger.warning(f"Ignoring override for '{key}': unsupported type {type(value)}")
                          continue # Ne pas ajouter aux applied_overrides
                # Ajouter d'autres traitements spécifiques si nécessaire
                # Pour les autres clés, on écrase simplement
                else:
                    final_args[key] = value
                applied_overrides[key] = final_args[key] # Log la valeur traitée
            else:
                logger.warning(f"Unknown override option '{key}' specified for SFTP connection '{connection_name}'. It might be ignored by pysftp.")
                # On pourrait choisir de l'ajouter quand même si pysftp le supporte via kwargs, mais plus sûr d'ignorer
                # final_args[key] = value # Ligne à ajouter si on veut passer des options inconnues

        if applied_overrides:
             logger.info(f"Applied SFTP option overrides for '{connection_name}': {applied_overrides}")
        else:
             logger.debug(f"No applicable SFTP options overrides found for '{connection_name}'.")

    # Retourner le dictionnaire d'arguments prêts pour SftpHandler/pysftp.Connection
    return final_args

class SftpHandler:
    """
    A wrapper for performing SFTP operations using pysftp.
    Uses a context manager for connection handling.
    """
    def __init__(self, **connection_args):
        """
        Initializes the handler with connection arguments prepared by
        get_sftp_connection_details. Connection is established later.

        Args:
            **connection_args: Dictionary containing host, port, username, etc.
                               including potentially overridden options.
        """
        # Valider les arguments essentiels reçus
        self.host = connection_args.get("host")
        self.port = connection_args.get("port", 22)
        self.username = connection_args.get("username")
        if not self.host or not self.username:
             raise ValueError("SftpHandler requires 'host' and 'username' in arguments.")

        # Stocker tous les arguments pour les passer à pysftp.Connection
        self.connection_args = connection_args
        self._connection = None
        logger.info(f"SftpHandler initialized for host: {self.host}:{self.port} user: {self.username}")
        # Log les cnopts utilisés s'ils existent dans les args
        cnopts_used = self.connection_args.get("cnopts", None)
        if cnopts_used and not cnopts_used.hostkeys:
             logger.warning("SFTP Host key verification is disabled (cnopts.hostkeys=None)! Insecure.")

    def __enter__(self):
        logger.info(f"Connecting to SFTP host: {self.host}:{self.port}...")
        try:
            # Passer tous les arguments préparés à pysftp.Connection
            self._connection = pysftp.Connection(**self.connection_args)
            logger.info("SFTP connection established successfully.")
            return self
        except Exception as e:
            logger.error(f"Failed to establish SFTP connection to {self.host}:{self.port}: {e}", exc_info=True)
            self._connection = None
            raise ConnectionError(f"SFTP connection failed for {self.host}:{self.port}") from e


    def __enter__(self):
        """Establishes the SFTP connection when entering the context."""
        logger.info(f"Connecting to SFTP host: {self.host}:{self.port}...")
        try:
            self._connection = pysftp.Connection(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                private_key=self.private_key,
                cnopts=self.cnopts
            )
            logger.info("SFTP connection established successfully.")
            return self # Return the handler instance itself
        except Exception as e:
            logger.error(f"Failed to establish SFTP connection to {self.host}:{self.port}: {e}", exc_info=True)
            # Ensure connection is None if failed
            self._connection = None
            # Re-raise exception to signal connection failure
            raise ConnectionError(f"SFTP connection failed for {self.host}:{self.port}") from e

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closes the SFTP connection when exiting the context."""
        if self._connection:
            logger.info(f"Closing SFTP connection to {self.host}:{self.port}...")
            self._connection.close()
            self._connection = None
            logger.info("SFTP connection closed.")
        # Return False to propagate exceptions if they occurred within the 'with' block
        return False

    def _ensure_connected(self):
        """Checks if the connection is active (must be called within a 'with' block)."""
        if not self._connection:
            # This should ideally not happen if used correctly with 'with' statement
            raise ConnectionError("SFTP connection is not active. Use within a 'with SftpHandler(...) as sftp:' block.")

    def download_file(self, remote_path: str, local_path: str, preserve_mtime: bool = False):
        """Downloads a file from the remote server."""
        self._ensure_connected()
        logger.info(f"Downloading SFTP file from '{remote_path}' to '{local_path}'")
        try:
            # Ensure local directory exists
            local_dir = os.path.dirname(local_path)
            if local_dir and not os.path.exists(local_dir):
                 os.makedirs(local_dir)
                 logger.debug(f"Created local directory: {local_dir}")

            self._connection.get(remote_path, local_path, preserve_mtime=preserve_mtime)
            logger.info(f"Successfully downloaded '{remote_path}'")
        except Exception as e:
            logger.error(f"Failed to download SFTP file '{remote_path}': {e}", exc_info=True)
            raise

    def upload_file(self, local_path: str, remote_path: str, preserve_mtime: bool = False, confirm: bool = True):
        """Uploads a local file to the remote server."""
        self._ensure_connected()
        logger.info(f"Uploading local file '{local_path}' to SFTP '{remote_path}'")
        if not os.path.exists(local_path):
             raise FileNotFoundError(f"Local file not found: {local_path}")
        try:
            # Ensure remote directory exists
            remote_dir = os.path.dirname(remote_path)
            if remote_dir and not self._connection.exists(remote_dir):
                 self._connection.makedirs(remote_dir)
                 logger.debug(f"Created remote directory: {remote_dir}")

            self._connection.put(local_path, remote_path, preserve_mtime=preserve_mtime, confirm=confirm)
            logger.info(f"Successfully uploaded to '{remote_path}'")
        except Exception as e:
            logger.error(f"Failed to upload SFTP file to '{remote_path}': {e}", exc_info=True)
            raise

    def list_dir(self, remote_path: str = '.') -> list:
        """Lists the contents of a remote directory."""
        self._ensure_connected()
        logger.debug(f"Listing directory: {remote_path}")
        try:
            return self._connection.listdir(remote_path)
        except Exception as e:
            logger.error(f"Failed to list SFTP directory '{remote_path}': {e}", exc_info=True)
            raise

    def list_dir_attr(self, remote_path: str = '.') -> list:
        """Lists the contents of a remote directory with attributes."""
        self._ensure_connected()
        logger.debug(f"Listing directory with attributes: {remote_path}")
        try:
            return self._connection.listdir_attr(remote_path)
        except Exception as e:
            logger.error(f"Failed to list SFTP directory attributes '{remote_path}': {e}", exc_info=True)
            raise

    def path_exists(self, remote_path: str) -> bool:
         """Checks if a path (file or directory) exists on the remote server."""
         self._ensure_connected()
         logger.debug(f"Checking existence of remote path: {remote_path}")
         try:
              return self._connection.exists(remote_path)
         except Exception as e:
              # Some servers might error on exists check depending on permissions/path
              logger.warning(f"Could not reliably check existence of '{remote_path}' (อาจจะเกิดจาก permissions): {e}")
              # Return False on error for safety, or re-raise depending on desired behavior
              return False

    def is_dir(self, remote_path: str) -> bool:
         """Checks if a remote path is a directory."""
         self._ensure_connected()
         try:
              return self._connection.isdir(remote_path)
         except Exception as e:
              logger.warning(f"Could not determine if '{remote_path}' is a directory: {e}")
              return False

    def is_file(self, remote_path: str) -> bool:
         """Checks if a remote path is a regular file."""
         self._ensure_connected()
         try:
              return self._connection.isfile(remote_path)
         except Exception as e:
              logger.warning(f"Could not determine if '{remote_path}' is a file: {e}")
              return False

    def make_dir(self, remote_path: str):
         """Creates a directory on the remote server (including intermediate dirs)."""
         self._ensure_connected()
         logger.info(f"Creating remote directory: {remote_path}")
         try:
              self._connection.makedirs(remote_path)
              logger.info(f"Successfully created or ensured directory exists: {remote_path}")
         except Exception as e:
              logger.error(f"Failed to create remote directory '{remote_path}': {e}", exc_info=True)
              raise