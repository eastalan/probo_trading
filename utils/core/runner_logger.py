import os
import logging
import datetime
import inspect
from functools import wraps

class FotMobRunnerLogger:
    """
    Enhanced logger for fotmob_runner.py that includes function names and datetime in filename.
    Only logs program execution information, not WebSocket data.
    """
    
    def __init__(self, base_name="fotmob_runner"):
        self.base_name = base_name
        self.logger = None
        self._setup_logger()
    
    def _setup_logger(self):
        """Setup the logger with datetime filename and proper formatting."""
        # Create logger
        self.logger = logging.getLogger(f"{self.base_name}_logger")
        self.logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # Create log filename with datetime
        current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"{self.base_name}_{current_datetime}.log"
        
        # Create log directory
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        log_dir = os.path.join("logs", "runner_logs", current_date)
        os.makedirs(log_dir, exist_ok=True)
        
        # Full log file path
        log_file_path = os.path.join(log_dir, log_filename)
        
        # Create file handler
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.INFO)
        
        # Create console handler for important messages
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter with function name
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Log the startup
        self.logger.info(f"FotMob Runner Logger initialized - Log file: {log_file_path}")
    
    def get_caller_info(self):
        """Get information about the calling function."""
        frame = inspect.currentframe()
        try:
            # Go up the stack to find the actual caller (skip this method and the wrapper)
            caller_frame = frame.f_back.f_back
            if caller_frame:
                return caller_frame.f_code.co_name, caller_frame.f_lineno
            return "unknown", 0
        finally:
            del frame
    
    def info(self, message):
        """Log info message with caller function name."""
        func_name, line_no = self.get_caller_info()
        # Create a LogRecord manually to set the function name
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, "", line_no, message, (), None, func_name
        )
        self.logger.handle(record)
    
    def warning(self, message):
        """Log warning message with caller function name."""
        func_name, line_no = self.get_caller_info()
        record = self.logger.makeRecord(
            self.logger.name, logging.WARNING, "", line_no, message, (), None, func_name
        )
        self.logger.handle(record)
    
    def error(self, message):
        """Log error message with caller function name."""
        func_name, line_no = self.get_caller_info()
        record = self.logger.makeRecord(
            self.logger.name, logging.ERROR, "", line_no, message, (), None, func_name
        )
        self.logger.handle(record)
    
    def debug(self, message):
        """Log debug message with caller function name."""
        func_name, line_no = self.get_caller_info()
        record = self.logger.makeRecord(
            self.logger.name, logging.DEBUG, "", line_no, message, (), None, func_name
        )
        self.logger.handle(record)

def log_function_entry_exit(logger):
    """Decorator to log function entry and exit."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"Entering function: {func.__name__}")
            try:
                result = func(*args, **kwargs)
                logger.info(f"Exiting function: {func.__name__} - Success")
                return result
            except Exception as e:
                logger.error(f"Exiting function: {func.__name__} - Error: {str(e)}")
                raise
        return wrapper
    return decorator

# Global logger instance
runner_logger = FotMobRunnerLogger()
