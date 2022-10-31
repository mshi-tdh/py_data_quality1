from datetime import datetime
import logging
import sys

LOG_LEVEL = logging._nameToLevel["DEBUG"]
def getlogger():
    file_formatter = logging.Formatter('[%(asctime)s] %(filename)s {%(funcName)s} %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(levelname)s -- %(message)s')
    #now = datetime.now()
    log_file_name = f"DQlogs.log"
    #log_file_name = f"logfile_{now.strftime('%Y%m%d%H%M%S')}.log"
    file_handler = logging.FileHandler(log_file_name,mode="w")
    #file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(file_formatter)
    console_handler = logging.StreamHandler()
    #console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    logger = logging.getLogger("dataquality")
    if not logger.hasHandlers():
        logger.setLevel(LOG_LEVEL)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    #logger.setLevel(LOG_LEVEL)
    #logger.addHandler(file_handler)
    #logger.addHandler(console_handler)
    return logger