# utils/logger_config.py
import logging
import os
from logging.handlers import RotatingFileHandler

# Create logs directory if missing
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "app.log")

# Common formatter
FORMATTER = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def get_logger(name: str) -> logging.Logger:
    """Returns a shared logger configured for the entire app."""
    logger = logging.getLogger(name)

    if not logger.hasHandlers():  # Avoid duplicate handlers in Streamlit reruns
        logger.setLevel(logging.INFO)

        # Console output (for Streamlit/log viewer)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(FORMATTER)

        # Rotating file output
        file_handler = RotatingFileHandler(
            LOG_FILE, maxBytes=2_000_000, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(FORMATTER)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
