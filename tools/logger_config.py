import logging
import os

def initiate_logger(log_file_path='test.log', name="logger"):
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    logger = logging.getLogger(name)

    if logger.hasHandlers():
        logger.handlers.clear() 

    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file_path, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s : %(message)s"))
    console_handler.setFormatter(logging.Formatter("[%(levelname)s] : %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


    return logger
