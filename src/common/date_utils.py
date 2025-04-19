# src/common/date_utils.py

import logging
from datetime import date, timedelta, datetime

# Importez la fonction de lecture depuis le module metadata
try:
    # On garde l'import spécifique pour l'instant
    from common import metadata as common_metadata # Utilise le module commun
except ImportError:
    logging.warning("Could not import specific job metadata module. Catchup logic might require specific implementation.")
    zebra_metadata = None

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%d" # Standard ISO date format

def _parse_date(date_str):
    """Parses a string into a date object using DATE_FORMAT."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, DATE_FORMAT).date()
    except ValueError:
        logger.error(f"Invalid date format for string '{date_str}'. Expected format: {DATE_FORMAT}.")
        raise

def _generate_date_range(start_date, end_date):
    """Generates a list of dates from start_date to end_date (inclusive)."""
    if not start_date or not end_date or start_date > end_date:
        return []
    return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

def _calculate_catchup_dates(last_processed_date, yesterday):
    """
    Helper function to calculate the date range for catchup.
    Used by both 'daily' (auto-catchup) and 'catchup' modes.

    Args:
        last_processed_date (date or None): The last successfully processed date.
        yesterday (date): Yesterday's date.

    Returns:
        list[date]: List of dates to process for catchup.
    """
    if last_processed_date is None:
        # No previous successful run found. Default to processing only yesterday.
        # This prevents accidental large loads on the first 'daily' run.
        # An 'initial' run should be performed first ideally.
        logger.warning("No last processed date found in metadata. Defaulting to process only yesterday: %s", yesterday.isoformat())
        return [yesterday]
    elif last_processed_date >= yesterday:
        # Already up-to-date.
        logger.info("Last processed date (%s) is up-to-date with yesterday (%s). No dates to process.",
                    last_processed_date.isoformat(), yesterday.isoformat())
        return []
    else:
        # Calculate the range from the day *after* the last processed date up to yesterday
        start_catchup_date = last_processed_date + timedelta(days=1)
        end_catchup_date = yesterday
        processing_dates = _generate_date_range(start_catchup_date, end_catchup_date)
        logger.info("Catchup required: Processing dates from %s to %s. Count: %d",
                    start_catchup_date.isoformat(), end_catchup_date.isoformat(), len(processing_dates))
        return processing_dates


def get_processing_dates(mode, config, start_date_str=None, end_date_str=None, processing_date_str=None):
    """
    Determines the list of dates to process based on the execution mode.
    Uses the common metadata module.

    Args:
        mode (str): Execution mode ('initial', 'daily', 'catchup', 'specific_date').
        config (dict): The full application configuration dictionary.
        start_date_str (str, optional): Start date string for 'initial' mode.
        end_date_str (str, optional): End date string for 'initial' mode.
        processing_date_str (str, optional): Specific date string for 'specific_date' mode.

    Returns:
        list[datetime.date]: A list of date objects to be processed. Sorted ascending.

    Raises:
        ValueError: If required arguments/config for a mode are missing or invalid.
        ImportError: If common_metadata module is unavailable.
    """
    today = date.today()
    yesterday = today - timedelta(days=1)
    processing_dates = []

    logger.info(f"Calculating processing dates for mode: {mode}")

    # Check if metadata module is available if needed
    if mode in ['daily', 'catchup']:
        if not common_metadata:
             raise ImportError(f"Mode '{mode}' requires the common.metadata module, but it is not available.")
        # Check if necessary config keys for metadata exist
        if not config.get("metadata", {}).get("job_name_identifier"):
             raise ValueError(f"Mode '{mode}' requires 'job_name_identifier' in the 'metadata' config section.")


    if mode == 'daily':
        logger.info("Daily mode selected. Checking for auto-catchup requirement...")
        # Read last processed date using the common metadata module and the config
        last_processed_date = common_metadata.read_last_processed_date(config)
        processing_dates = _calculate_catchup_dates(last_processed_date, yesterday)
        # ... (logging comme avant) ...

    elif mode == 'catchup':
        logger.info("Explicit Catchup mode selected.")
        last_processed_date = common_metadata.read_last_processed_date(config)
        processing_dates = _calculate_catchup_dates(last_processed_date, yesterday)

    elif mode == 'specific_date':
        # ... (pas de changement dans la logique ici, n'utilise pas metadata) ...
        if not processing_date_str:
            raise ValueError("Argument --processing-date is required for 'specific_date' mode.")
        try:
            specific_date = _parse_date(processing_date_str)
            if specific_date >= today:
                 logger.warning(f"Specific date {specific_date.isoformat()} is today or in the future. No data will be processed.")
                 processing_dates = []
            else:
                 processing_dates = [specific_date]
                 logger.info(f"Specific Date mode: Processing date set to: {specific_date.isoformat()}")
        except ValueError:
             raise ValueError(f"Invalid format for --processing-date: '{processing_date_str}'. Use YYYY-MM-DD.")

    elif mode == 'initial':
        # ... (pas de changement dans la logique ici, n'utilise pas metadata) ...
        if not start_date_str or not end_date_str:
            raise ValueError("Arguments --start-date and --end-date are required for 'initial' mode.")
        try:
            start_date = _parse_date(start_date_str)
            end_date = _parse_date(end_date_str)
        except ValueError:
            raise ValueError(f"Invalid date format in --start-date or --end-date. Use YYYY-MM-DD.")
        if start_date > end_date:
            raise ValueError(f"Start date ({start_date_str}) cannot be after end date ({end_date_str}).")
        effective_end_date = min(end_date, yesterday)
        if start_date > effective_end_date:
             logger.warning(f"Start date ({start_date_str}) is after the effective end date ({effective_end_date.isoformat()}). No past dates to process.")
             processing_dates = []
        else:
             processing_dates = _generate_date_range(start_date, effective_end_date)
             logger.info(f"Initial mode: Processing dates from {start_date.isoformat()} to {effective_end_date.isoformat()}. Count: {len(processing_dates)}")

    else:
        raise ValueError(f"Unknown execution mode: {mode}")

    processing_dates.sort()
    logger.info(f"Final list of processing dates (YYYY-MM-DD): {[d.isoformat() for d in processing_dates]}")
    return processing_dates

# --- Example usage for testing ---
if __name__ == '__main__':
    print("Testing date utility functions (with auto-catchup in daily)...")

    # Dummy metadata module for testing catchup
    class MockMetadata:
        _last_date = None
        def read_last_processed_date(self, config):
            print(f"  (MockMetadata) Reading last processed date: {self._last_date}")
            return self._last_date
        def set_last_date(self, dt):
            self._last_date = dt
            print(f"  (MockMetadata) Setting last date to: {dt}")

    zebra_metadata = MockMetadata()
    mock_meta_config = {"tracker_file_path": "dummy_path.json"}
    today = date.today()
    yesterday = today - timedelta(days=1)

    # --- Test cases for Daily mode ---
    print("\n--- Testing Daily (No metadata - first run) ---")
    zebra_metadata.set_last_date(None)
    dates = get_processing_dates('daily', mock_meta_config)
    assert len(dates) == 1 # Should default to yesterday
    assert dates[0] == yesterday

    print("\n--- Testing Daily (Up-to-date) ---")
    zebra_metadata.set_last_date(yesterday) # Last processed was yesterday
    dates = get_processing_dates('daily', mock_meta_config)
    assert len(dates) == 0 # Should be empty

    print("\n--- Testing Daily (Missed 1 day) ---")
    day_before_yesterday = yesterday - timedelta(days=1)
    zebra_metadata.set_last_date(day_before_yesterday) # Last processed day before yesterday
    dates = get_processing_dates('daily', mock_meta_config)
    assert len(dates) == 1 # Should process only yesterday
    assert dates[0] == yesterday

    print("\n--- Testing Daily (Missed 3 days - Auto Catchup) ---")
    three_days_ago = yesterday - timedelta(days=3)
    zebra_metadata.set_last_date(three_days_ago) # Last processed 4 days before today
    dates = get_processing_dates('daily', mock_meta_config)
    assert len(dates) == 3 # Process day 2, 1 ago (relative to today), which is yesterday and the two before it
    assert dates[0] == yesterday - timedelta(days=2)
    assert dates[1] == yesterday - timedelta(days=1)
    assert dates[2] == yesterday

    # --- Test cases for Catchup mode (should behave similarly) ---
    print("\n--- Testing Catchup (No metadata) ---")
    zebra_metadata.set_last_date(None)
    dates = get_processing_dates('catchup', mock_meta_config)
    assert len(dates) == 1
    assert dates[0] == yesterday

    print("\n--- Testing Catchup (Up-to-date) ---")
    zebra_metadata.set_last_date(yesterday)
    dates = get_processing_dates('catchup', mock_meta_config)
    assert len(dates) == 0

    print("\n--- Testing Catchup (Missed 3 days) ---")
    zebra_metadata.set_last_date(three_days_ago)
    dates = get_processing_dates('catchup', mock_meta_config)
    assert len(dates) == 3
    assert dates[0] == yesterday - timedelta(days=2)
    assert dates[-1] == yesterday

    # --- Other modes remain the same ---
    print("\n--- Testing Specific Date (Yesterday) ---")
    test_specific_date = yesterday.isoformat()
    dates = get_processing_dates('specific_date', mock_meta_config, processing_date_str=test_specific_date)
    assert len(dates) == 1
    assert dates[0] == yesterday

    print("\n--- Testing Specific Date (Today - should yield no dates) ---")
    test_specific_date = today.isoformat()
    dates = get_processing_dates('specific_date', mock_meta_config, processing_date_str=test_specific_date)
    assert len(dates) == 0

    print("\n--- Testing Initial (Last 5 days ending yesterday) ---")
    start = (today - timedelta(days=5)).isoformat()
    end = yesterday.isoformat()
    dates = get_processing_dates('initial', mock_meta_config, start_date_str=start, end_date_str=end)
    assert len(dates) == 5 # 5, 4, 3, 2, 1 days ago
    assert dates[0] == date.fromisoformat(start)
    assert dates[-1] == yesterday

    print("\nDate utility tests complete.")