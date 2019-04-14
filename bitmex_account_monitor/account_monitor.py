import sys
import atexit
import signal

from time import sleep
from datetime import timedelta, datetime

import json

import pymongo
import redis

from pybitmex import BitMEXClient

from bitmex_account_monitor.utils import log, constants
from bitmex_account_monitor.utils.settings import settings


logger = log.setup_custom_logger('account_monitor')


def now():
    return datetime.now().astimezone(constants.TIMEZONE)


class AccountMonitor:

    def __init__(self):
        self.instance_name = settings.API_KEY[:8]

        # Client to the BitMex exchange.
        logger.info("Connecting to BitMEX exchange: %s %s" % (settings.BASE_URL, settings.SYMBOL))
        self.bitmex_client = BitMEXClient(
            settings.BASE_URL, settings.SYMBOL,
            api_key=settings.API_KEY, api_secret=settings.API_SECRET,
            use_websocket=True, use_rest=True,
            subscriptions=["instrument", "order", "position", "margin"],
            order_id_prefix="",
            agent_name=None,
            http_timeout=None,
            expiration_seconds=settings.API_EXPIRATION_SECONDS
        )

        # MongoDB client.
        logger.info("Connecting to %s" % settings.MONGO_DB_URI)
        self.mongo_client = pymongo.MongoClient(settings.MONGO_DB_URI)

        # Redis client.
        logger.info("Connecting to Redis %s:%d/%d" % (settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_DB))
        self.redis = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)

        # Now the clients are all up.
        self.is_running = True

        """
        Once db, redis and exchange clients are created,
        register exit handler that will always release resources on any error.
        """
        atexit.register(self.exit)
        signal.signal(signal.SIGTERM, self.exit)

        self.sanity_check()

    def sanity_check(self):
        # Ensure market is open.
        if not self.bitmex_client.is_market_in_normal_state():
            raise Exception("Market state error: " + self.bitmex_client.ws_market_state())

    def exit(self, p1=None, p2=None, p3=None):
        if not self.is_running:
            return
        try:
            if p1 or p2 or p3:
                logger.debug("exit(%s %s %s)", str(p1), str(p2), str(p3))
        except Exception as _:
            pass

        logger.info('SHUTTING DOWN BitMEX Market Maker. Version %s' % constants.VERSION)
        try:
            self.mongo_client.close()
        except Exception as e:
            logger.info("Unable to close MongoDB client: %s", str(e))

        try:
            # We don't close orders at exit.
            # self.bitmex_client.cancel_all_orders()
            self.bitmex_client.close()
        except Exception as e:
            logger.info("Unable to close BitMEX client: %s", str(e))

        # Now the clients are all down.
        self.is_running = False

        sleep(1)
        sys.exit()

    def run_loop(self):
        loop_count = 0
        try:
            while True:
                pass
                loop_id = now().strftime("%Y%m%d%H%M%S_") + str(loop_count)
                loop_count += 1
                logger.info(
                    "LOOP[%s] (%s) {INTERVAL: %.2f; ACCOUNT: %s}",
                    loop_id, constants.VERSION, settings.LOOP_INTERVAL_SECONDS, settings.BITMEX_ACCOUNT_ID
                )
                sleep(settings.LOOP_INTERVAL_SECONDS)
        except Exception as e:
            import sys
            import traceback
            traceback.print_exc(file=sys.stdout)

            logger.warning("Error: %s" % str(e))
            logger.info(sys.exc_info())
        finally:
            self.exit()


def start():
    logger.info('STARTING BitMEX Account Monitor. Version %s' % constants.VERSION)

    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        mm = AccountMonitor()
        mm.run_loop()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
