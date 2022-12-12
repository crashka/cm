# -*- coding: utf-8 -*-

"""Core module - shared/common resources
"""

from os import environ
import os.path
from socket import gethostname
import logging
import logging.handlers

from .utils import Config

#########
# types #
#########

# collection of unmapped objects (i.e. `Collection` minus str-like and dict-like)
ObjCollect = set | list | tuple

############################
# config/environment stuff #
############################

FILE_DIR      = os.path.dirname(os.path.realpath(__file__))
BASE_DIR      = os.path.realpath(os.path.join(FILE_DIR, os.pardir))
CONFIG_DIR    = 'config'
CONFIG_FILE   = 'config.yml'
CONFIG_PATH   = os.path.join(BASE_DIR, CONFIG_DIR, CONFIG_FILE)
cfg           = Config(CONFIG_PATH)

ENV_ID        = "%s:%s" % (gethostname(), BASE_DIR)
env           = cfg.config('environment').get(ENV_ID) or {}
env_overrides = {'database': 'CM_DBNAME'}
env.update({k: environ[v] for k, v in env_overrides.items() if v in environ})

###########
# logging #
###########

# create logger (TODO: logging parameters belong in config file as well!!!)
LOGGER_NAME  = 'cm'
LOG_DIR      = 'log'
LOG_FILE     = LOGGER_NAME + '.log'
LOG_PATH     = os.path.join(BASE_DIR, LOG_DIR, LOG_FILE)
LOG_FMTR     = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)s]: %(message)s')
LOG_FILE_MAX = 50000000
LOG_FILE_NUM = 50

dflt_hand = logging.handlers.RotatingFileHandler(LOG_PATH, 'a', LOG_FILE_MAX, LOG_FILE_NUM)
dflt_hand.setLevel(logging.DEBUG)
dflt_hand.setFormatter(LOG_FMTR)

dbg_hand = logging.StreamHandler()
dbg_hand.setLevel(logging.DEBUG)
dbg_hand.setFormatter(LOG_FMTR)

log = logging.getLogger(LOGGER_NAME)
log.setLevel(logging.INFO)
log.addHandler(dflt_hand)

##############
# Exceptions #
##############

class DataError(RuntimeError):
    """Thrown if there is a problem detected with any of the data at runtime,
    whether due to bad and/or insufficient external data or errrant internal
    data processing
    """
    pass

class ConfigError(RuntimeError):
    """Thrown if there is a problem with a config file entry, or combination
    of entries
    """
    pass

class LogicError(RuntimeError):
    """Basically the same as an assert, but with a `raise` interface
    """
    pass

class ImplementationError(RuntimeError):
    """Thrown if there is a problem implementing an internal interface (e.g.
    `Parser` subclass)
    """
    pass

############
# defaults #
############

# kindly internet fetch interval (TODO: move to config file!!!)
DFLT_FETCH_INT   = 1.0
DFLT_HTML_PARSER = 'lxml'
#DFLT_HTML_PARSER = 'html.parser'  # sometimes treats <br /> as an opening tag--WRONG!!!
