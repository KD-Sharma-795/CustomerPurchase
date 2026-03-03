import logging
import os
import json
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Step 1: Create logs folder
LOG_DIR = "logs"
LOG_DIR_PATH = os.path.join(os.getcwd(), LOG_DIR)
os.makedirs(LOG_DIR_PATH, exist_ok=True)

# Step 2: Create log file
LOG_FILE = f"{datetime.now().strftime('%Y_%m_%d')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR_PATH, LOG_FILE)

# Step 3: Custom JSON Formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "module": record.name,
            "line_no": record.lineno,
            "message": record.getMessage(),
        }
        return json.dumps(log_record)


# Step 4: Configure Logger

def get_logger(name=__name__):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # 🔹 File Handler (Rotating)
    file_handler = RotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5
    )
    file_handler.setFormatter(JsonFormatter())

    # 🔹 Console Handler (Table Style)
    console_handler = logging.StreamHandler()
    console_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | Line:%(lineno)-3d | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_format)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
