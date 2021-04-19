
import configparser
import logging
import os
import time
from pathlib import Path

PARENT_PATH = os.fspath(Path(__file__).parents[0])
LOGGING_FILE_PATH = os.path.join(PARENT_PATH, "../__logger", "{}.log")


def config_reader(file_path, section):
    """
    set directory and path and return the path
    :param file_path:
    :type file_path:
    :param section:
    :type section:
    :return:
    :rtype:
    """
    config = configparser.ConfigParser()
    print(str(config))
    config.read(file_path)
    return dict(config.items(section))


def set_logger(file_path_extension):
    '''A logging helper.
    Keeps the logged experiments in the __logger path.
    Both prints out on the Terminal and writes on the
    .log file.'''
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)-7s: %(levelname)-1s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(
                LOGGING_FILE_PATH.format(file_path_extension)
            ),
            logging.StreamHandler()
        ])
    return logging
