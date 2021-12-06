#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    '''
    This function enables creation of multiple loggers
    Source: 
    https://stackoverflow.com/questions/11232230/logging-to-two-files-with-different-settings

    '''

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger