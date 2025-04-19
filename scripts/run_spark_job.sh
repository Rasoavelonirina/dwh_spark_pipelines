#!/bin/bash

# --- Configuration - Adjust these variables ---
# SPARK_MASTER="yarn" # Or "local[*]", "spark://host:port", "k8s://..." etc.
SPARK_MASTER="local[*]" # Example: Use local mode for simple testing first
# SPARK_DEPLOY_MODE="cluster" # Or "client"
SPARK_DEPLOY_MODE="client" # Usually "client" for testing/dev, "cluster" for production on YARN/K8s

# Project Structure - Assuming script is run from project root (data_pipelines_project/)
PROJECT_ROOT=$(pwd) # Assumes you run './scripts/run_spark_job.sh ...' from the root
SRC_DIR="${PROJECT_ROOT}/src"
CONFIG_DIR="${PROJECT_ROOT}/config"
# Path to the main Python file for the job (relative to SRC_DIR potentially needed for python path)
# We submit the main.py of the specific job
# JOB_MAIN_PY="jobs/zebra_last_transaction/main.py" # Path relative to SRC_DIR

# Packaging (Optional but recommended for non-local modes)
# Create a zip file of the 'src' directory contents (common and jobs modules)
# echo "Creating source code archive..."
# cd "${SRC_DIR}" || exit 1 # Go into src to avoid including 'src' folder itself in zip
# zip -r "${PROJECT_ROOT}/source_code.zip" ./*
# cd "${PROJECT_ROOT}" || exit 1
# PY_FILES_ARG="--py-files ${PROJECT_ROOT}/source_code.zip"
PY_FILES_ARG="" # Set to empty if running in local mode and src is accessible

# Dependencies (e.g., JDBC Driver Jar) - Adjust path and filename
MARIADB_JDBC_DRIVER_PATH="/path/to/your/jars/mariadb-java-client-3.x.x.jar" # IMPORTANT: Set this path!
JARS_ARG="--jars ${MARIADB_JDBC_DRIVER_PATH}"
# OR use --packages if your cluster has internet access (adjust version)
# PACKAGES_ARG="--packages org.mariadb.jdbc:mariadb-java-client:3.1.4" # Example package coordinate

# Default Spark Resources (can be overridden by job config or command line)
DEFAULT_DRIVER_MEMORY="1g"
DEFAULT_EXECUTOR_MEMORY="2g"
DEFAULT_EXECUTOR_CORES=1
DEFAULT_NUM_EXECUTORS=2 # Example for YARN/K8s, ignored in local mode

# --- Argument Parsing ---
JOB_NAME=""
EXECUTION_MODE=""
START_DATE=""
END_DATE=""
PROCESSING_DATE=""
JOB_CONFIG_FILE_NAME="" # Just the filename like 'zebra_last_transaction.yaml'

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --job-name) JOB_NAME="$2"; shift ;;
        --execution-mode) EXECUTION_MODE="$2"; shift ;;
        --start-date) START_DATE="$2"; shift ;;
        --end-date) END_DATE="$2"; shift ;;
        --processing-date) PROCESSING_DATE="$2"; shift ;;
        --job-config) JOB_CONFIG_FILE_NAME="$2"; shift ;;
        # Add other potential spark-submit args overrides if needed (--driver-memory, etc.)
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# --- Validation ---
if [ -z "$JOB_NAME" ]; then
  echo "Error: --job-name is required."
  exit 1
fi
if [ -z "$EXECUTION_MODE" ]; then
  echo "Error: --execution-mode is required."
  exit 1
fi
if [ -z "$JOB_CONFIG_FILE_NAME" ]; then
   # Default config name based on job name if not provided
   JOB_CONFIG_FILE_NAME="${JOB_NAME}.yaml"
   echo "Warning: --job-config not specified, defaulting to ${JOB_CONFIG_FILE_NAME}"
fi

# Construct paths based on job name
JOB_MAIN_PY_PATH="${SRC_DIR}/jobs/${JOB_NAME}/main.py"
JOB_CONFIG_PATH="${CONFIG_DIR}/jobs/${JOB_CONFIG_FILE_NAME}"
COMMON_CONFIG_DIR_PATH="${CONFIG_DIR}/common" # Assumed standard path

if [ ! -f "$JOB_MAIN_PY_PATH" ]; then
    echo "Error: Main python file not found for job '$JOB_NAME' at $JOB_MAIN_PY_PATH"
    exit 1
fi
if [ ! -f "$JOB_CONFIG_PATH" ]; then
    echo "Error: Job config file not found: $JOB_CONFIG_PATH"
    exit 1
fi
if [ ! -d "$COMMON_CONFIG_DIR_PATH" ]; then
    echo "Error: Common config directory not found: $COMMON_CONFIG_DIR_PATH"
    exit 1
fi
# Check for JDBC driver if JARS_ARG is used
if [[ -n "$JARS_ARG" && ! -f "$MARIADB_JDBC_DRIVER_PATH" ]]; then
   echo "Error: MariaDB JDBC Driver not found at specified path: $MARIADB_JDBC_DRIVER_PATH"
   # Comment out exit if using --packages instead
   exit 1
fi


# --- Build Arguments for main.py ---
APP_ARGS=(
    "--config-file" "$JOB_CONFIG_PATH"
    "--common-config-dir" "$COMMON_CONFIG_DIR_PATH"
    "--execution-mode" "$EXECUTION_MODE"
)
if [ -n "$START_DATE" ]; then
    APP_ARGS+=("--start-date" "$START_DATE")
fi
if [ -n "$END_DATE" ]; then
    APP_ARGS+=("--end-date" "$END_DATE")
fi
if [ -n "$PROCESSING_DATE" ]; then
    APP_ARGS+=("--processing-date" "$PROCESSING_DATE")
fi


# --- Execute spark-submit ---
echo "--------------------------------------------------"
echo "Submitting Spark Job: ${JOB_NAME}"
echo "Spark Master: ${SPARK_MASTER}"
echo "Deploy Mode: ${SPARK_DEPLOY_MODE}"
echo "Main Python File: ${JOB_MAIN_PY_PATH}"
echo "Job Config File: ${JOB_CONFIG_PATH}"
echo "Common Config Dir: ${COMMON_CONFIG_DIR_PATH}"
echo "Application Arguments: ${APP_ARGS[*]}"
echo "JDBC Jars: ${JARS_ARG:-N/A}"
# echo "Py Files: ${PY_FILES_ARG:-N/A}"
echo "--------------------------------------------------"

# Set PYTHONPATH to include the 'src' directory so imports like 'from common import ...' work
export PYTHONPATH="${PYTHONPATH}:${SRC_DIR}"
echo "PYTHONPATH set to: $PYTHONPATH"

spark-submit \
    --master "${SPARK_MASTER}" \
    --deploy-mode "${SPARK_DEPLOY_MODE}" \
    --name "${JOB_NAME}_${EXECUTION_MODE}" \
    --driver-memory "${DEFAULT_DRIVER_MEMORY}" \
    --executor-memory "${DEFAULT_EXECUTOR_MEMORY}" \
    --executor-cores "${DEFAULT_EXECUTOR_CORES}" \
    --num-executors "${DEFAULT_NUM_EXECUTORS}" \
    ${JARS_ARG} \
    # ${PACKAGES_ARG} # Use either --jars or --packages
    # ${PY_FILES_ARG} # Add if using zipped source code
    "${JOB_MAIN_PY_PATH}" \
    "${APP_ARGS[@]}" # Pass application arguments after the main script

# Capture exit code
EXIT_CODE=$?

echo "--------------------------------------------------"
echo "Spark job finished with exit code: ${EXIT_CODE}"
echo "--------------------------------------------------"

# Optional: Clean up zipped source code
# if [ -f "${PROJECT_ROOT}/source_code.zip" ]; then
#     echo "Removing source code archive..."
#     rm "${PROJECT_ROOT}/source_code.zip"
# fi

exit $EXIT_CODE