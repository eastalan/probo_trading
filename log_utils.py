import os
import datetime

def get_dated_log_path(base_name, filename):
    """
    Creates a dated log directory and returns the full path to the log file.
    
    Args:
        base_name (str): Base directory name (e.g., 'fotmob_data', 'socketdata')
        filename (str): Log filename (e.g., 'event.log')
    
    Returns:
        str: Full path to the log file in dated directory
    """
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    log_dir = os.path.join("logs", base_name, current_date)
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, filename)
