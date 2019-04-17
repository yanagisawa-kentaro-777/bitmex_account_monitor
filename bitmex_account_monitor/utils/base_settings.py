import os
import logging

########################################################################################################################
# Connection and authentication to the exchange and middle wares.
########################################################################################################################

# API URL.
# BASE_URL = "https://testnet.bitmex.com/api/v1/"
BASE_URL = "https://www.bitmex.com/api/v1/"

BITMEX_ACCOUNT_ID = os.environ['BITMEX_ACCOUNT_ID']
API_KEY = os.environ['BITMEX_API_KEY']
API_SECRET = os.environ['BITMEX_API_SECRET']

MONGO_DB_URI = "mongodb://mongo:27017/"

REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0

REDIS_ACCOUNT_POSITIONS_KEY = 'from-account-monitor:{}:positions'.format(BITMEX_ACCOUNT_ID)
REDIS_ACCOUNT_OPEN_ORDERS_KEY = 'from-account-monitor:{}:open-orders'.format(BITMEX_ACCOUNT_ID)
REDIS_ACCOUNT_BALANCES_KEY = 'from-account-monitor:{}:balances'.format(BITMEX_ACCOUNT_ID)

GRAPHITE_HOST = "graphite"
GRAPHITE_PORT = 2004


########################################################################################################################
# Target
########################################################################################################################

# Instrument to market make on BitMEX.
SYMBOL = "XBTUSD"

########################################################################################################################
# Misc Behavior
########################################################################################################################


def _get_float_from_env(key: str, default_value: float):
    s = os.environ.get(key)
    if not s or (len(s.strip()) <= 0):
        return default_value
    return float(s.strip())


INITIAL_SLEEP_SECONDS = _get_float_from_env('INITIAL_SLEEP_SECONDS', 0)
LOOP_INTERVAL_SECONDS = _get_float_from_env('LOOP_INTERVAL_SECONDS', 3)
WS_REFRESH_INTERVAL = _get_float_from_env('WS_REFRESH_INTERVAL', 600)

MAX_REFRESH_WAIT_SECONDS = 15

WS_POSITION_MAX_AGE_SECONDS = 8
WS_ORDER_MAX_AGE_SECONDS = 8
WS_BALANCE_MAX_AGE_SECONDS = 15

REST_AGENT_NAME = 'account-monitor'
HTTP_TIMEOUT_SECONDS = 10
API_EXPIRATION_SECONDS = 3600


########################################################################################################################
# Logging
########################################################################################################################
# Available levels: logging.(DEBUG|INFO|WARN|ERROR)
LOG_LEVEL = logging.INFO

# Logging to files is not recommended when you run this on Docker.
# By leaving the name empty the program avoids to create log files.
LOG_FILE_NAME = ''
