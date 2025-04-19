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


class SftpHandler:
    """
    A wrapper for performing SFTP operations using pysftp.
    Uses a context manager for connection handling.
    """
    def __init__(self, host: str, port: int, username: str, password: str = None, private_key: str = None, cnopts: pysftp.CnOpts = CNOPTS):
        """
        Initializes the handler with connection details. Connection is established
        when entering the context manager.

        Args:
            host (str): SFTP host address.
            port (int): SFTP port number.
            username (str): SFTP username.
            password (str, optional): SFTP password. Required if private_key is not used.
            private_key (str, optional): Path to the private key file. Required if password is not used.
            cnopts (pysftp.CnOpts, optional): Connection options (e.g., host key handling). Defaults to module's CNOPTS.
        """
        if not host:
            raise ValueError("SFTP host is required.")
        if not username:
            raise ValueError("SFTP username is required.")
        if not password and not private_key:
            raise ValueError("Either SFTP password or private_key path must be provided.")
        if password and private_key:
            logger.warning("Both password and private key provided. Pysftp might prioritize one.")

        self.host = host
        self.port = port
        self.username = username
        self.password = password # SECURITY WARNING: Password potentially stored in memory!
        self.private_key = private_key
        self.cnopts = cnopts
        self._connection = None # Connection object managed by context manager
        logger.info(f"SftpHandler initialized for host: {self.host}:{self.port} user: {self.username}")
        if not cnopts.hostkeys:
             logger.warning("SFTP Host key verification is disabled (cnopts.hostkeys=None)! This is insecure and should NOT be used in production.")


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