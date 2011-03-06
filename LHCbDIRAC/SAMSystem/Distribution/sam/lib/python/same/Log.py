#
# Module that implements loggging and console messages for the framework
#

import logging
from Config import config


# Logging levels
# Note: "WARNING" and "NOTSET" are not yet available in python 2.2
logcodes={ "CRITICAL": logging.CRITICAL,
           "ERROR": logging.ERROR,
           "INFO": logging.INFO,
           "DEBUG": logging.DEBUG,
         }

# Object that takes care for logging to a file
filelog=logging.getLogger('file_logger')
handler=logging.FileHandler(config.get("DEFAULT", "logdir") + "/same.log")       
formatter = logging.Formatter('%(asctime)s - [%(levelname)s] %(message)s', '%Y-%m-%d, %H:%M:%S')
handler.setFormatter(formatter)
    
filelog.addHandler(handler)    
filelog.setLevel( logcodes[config.get("DEFAULT", "loglevel")] )   


# Object that takes care for console messages
console=logging.getLogger('console_logger')
conshandler=logging.StreamHandler()       
consformatter = logging.Formatter('[%(levelname)s]: %(message)s')
conshandler.setFormatter(consformatter)
    
console.addHandler(conshandler)    
console.setLevel( logcodes[config.get("DEFAULT", "verbosity")] )   

#
# Functions to set logging level (needed for verbosity change options)
# Main function should be a private one (but since it's not a class method...)

def __set_loglvl(level):
    filelog.setLevel(level)
    console.setLevel(level)

#debug is special : don't log debug information in file when user requests debug
#If user wants to have debug information in logs, he must use the same.conf file
def set_debug_lvl():
    console.setLevel(logging.DEBUG)

def set_info_lvl():
    __set_loglvl(logging.INFO)

def set_error_lvl():
    __set_loglvl(logging.ERROR)

def set_crit_lvl():
    __set_loglvl(logging.CRITICAL)

#
# Library fuctions that write both to console and the log file
#

def debug (str):

    console.debug(str)
    filelog.debug(str)

def warn (str):

    console.warn(str)
    filelog.warn(str)

def info (str):

    console.info(str)
    filelog.info(str)


def error (str):

    console.error(str)
    filelog.error(str)


def critical (str):

    console.critical(str)
    filelog.critical(str)




