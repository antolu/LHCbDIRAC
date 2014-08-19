""" Configuratoin of grid collector
"""
import logging
LISTEN_ADDRESS = '0.0.0.0'
LISTEN_PORT = 9152

PID_FILE = '/var/run/eventindex-grid-collector.pid'

DOWNLOADS_BASE_URL = "https://eindex.cern.ch/data"
DOWNLOADS_CACHE_DIR = '/var/eventindex/download_cache'
DOWNLOADS_REQUEST_DIR = '/var/eventindex/requests'
STATUS_DONE = "done"
STATUS_NEW = "new"
STATUS_RUNNING = "running"
STATUS_FAIL = "fail"
STATUS_INVALID = "invalid"
IS_TESTING = False
TYPE_ROOT = "root"
TYPE_SE = "se"
LOGLEVEL = logging.DEBUG

# TESTING
# DOWNLOADS_CACHE_DIR = './cache'
# DOWNLOADS_REQUEST_DIR = './requests'
# PID_FILE = './eventindex-grid-collector.pid'
# IS_TESTING = True
