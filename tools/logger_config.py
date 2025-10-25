import logging

def get_logger(log_file_path='test.log', name="logger"):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():  # prevent duplicate handlers in notebooks
        logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(log_file_path, mode="w", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s : %(message)s"))
        console_handler.setFormatter(logging.Formatter("[%(levelname)s] : %(message)s"))

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
