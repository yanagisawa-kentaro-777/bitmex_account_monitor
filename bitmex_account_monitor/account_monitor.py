import sys
import atexit
import signal

from time import sleep
from datetime import timedelta, datetime, timezone

import json
from dateutil.parser import parse

import pymongo
import redis

from pybitmex import *

from bitmex_account_monitor.utils import log, constants
from bitmex_account_monitor.utils.settings import settings


logger = log.setup_custom_logger('account_monitor')


def now():
    return datetime.now().astimezone(constants.TIMEZONE)


def _format_datetime(dt: datetime):
    return dt.strftime("%Y-%m-%d_%H:%M:%S_%Z")


def _launder_datetime_string(s: str):
    _format_datetime(parse(s))


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
            agent_name=settings.REST_AGENT_NAME,
            http_timeout=settings.HTTP_TIMEOUT_SECONDS,
            expiration_seconds=settings.API_EXPIRATION_SECONDS
        )

        # Redis client.
        logger.info("Connecting to Redis %s:%d/%d" % (settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_DB))
        self.redis = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)

        # MongoDB client.
        logger.info("Connecting to %s" % settings.MONGO_DB_URI)
        self.mongo_client = pymongo.MongoClient(settings.MONGO_DB_URI)

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

    def _is_age_less_or_equal(self, table_name: str, std_time: datetime, max_allowable_seconds: float):
        ws_updated = self.bitmex_client.get_last_ws_update(table_name)
        if not ws_updated:
            return False
        return (std_time - ws_updated).total_seconds() <= max_allowable_seconds

    @staticmethod
    def _prune_position(json_array):
        for each in json_array:
            if each["symbol"] == settings.SYMBOL:
                return {"quantity": each["currentQty"], "leverage": each["leverage"],
                        "avgEntryPrice": each["avgEntryPrice"]}
        return None

    def _ws_read_current_position(self):
        json_array = self.bitmex_client.ws_raw_current_position()
        return self._prune_position(json_array)

    def _rest_get_current_position(self):
        json_array = self.bitmex_client.rest_get_raw_positions_of_account({'symbol': settings.SYMBOL})
        return self._prune_position(json_array)

    def read_current_position(self, tag, std_time: datetime):
        if self._is_age_less_or_equal('position', std_time, settings.WS_POSITION_MAX_AGE_SECONDS):
            # Read from ws.
            result = self._ws_read_current_position()
            if result:
                logger.info("[%s] Read position by WS.", tag)
                return result

        # Get by REST.
        logger.info("[%s] Read position by REST.", tag)
        return self._rest_get_current_position()

    @staticmethod
    def _prune_open_orders(json_array):
        def order_obj_from_json(json_obj):
            return OpenOrder(
                json_obj["orderID"], json_obj["clOrdID"],
                json_obj["side"], json_obj["orderQty"], json_obj["price"],
                parse(json_obj["timestamp"]).astimezone(timezone.utc)
            )

        bids = [order_obj_from_json(each) for each in json_array if each["side"] == "Buy"]
        asks = [order_obj_from_json(each) for each in json_array if each["side"] == "Sell"]
        return OpenOrders(
            bids=sorted(bids, key=lambda o: o.price, reverse=True),
            asks=sorted(asks, key=lambda o: o.price, reverse=False)
        )

    def _ws_read_open_orders(self):
        return self.bitmex_client.ws_open_order_objects_of_account()

    def _rest_get_open_orders(self):
        raw_open_orders = self.bitmex_client.rest_get_raw_orders_of_account({'open': True})
        return self._prune_open_orders(raw_open_orders)

    def read_open_orders(self, tag, std_time: datetime):
        if self._is_age_less_or_equal('order', std_time, settings.WS_ORDER_MAX_AGE_SECONDS):
            # Read from ws.
            result = self._ws_read_open_orders()
            if result:
                logger.info("[%s] Read open orders by WS.", tag)
                return result

        # Get by REST.
        logger.info("[%s] Read open orders by REST.", tag)
        return self._rest_get_open_orders()

    @staticmethod
    def open_orders_to_dict(open_orders: OpenOrders):
        def to_dict(order: OpenOrder):
            return {"orderID": order.order_id, "clientOrderID": order.client_order_id,
                    "side": order.side, "quantity": order.quantity, "price": order.price,
                    "timestamp": _format_datetime(order.timestamp)}
        bids = [to_dict(b) for b in open_orders.bids]
        asks = [to_dict(a) for a in open_orders.asks]
        return {"bids": bids, "asks": asks}

    @staticmethod
    def _prune_balance_object(json_obj):
        return {"withdrawableBalance": json_obj["withdrawableMargin"],
                "walletBalance": json_obj["walletBalance"],
                "timestamp": _launder_datetime_string(json_obj["timestamp"])}

    def _ws_read_balances(self):
        json_obj = self.bitmex_client.ws_raw_balances_of_account()
        return self._prune_balance_object(json_obj)

    def _rest_get_balances(self):
        json_obj = self.bitmex_client.rest_get_raw_margin_of_account()
        return self._prune_balance_object(json_obj)

    def read_balances(self, tag, std_time: datetime):
        if self._is_age_less_or_equal('margin', std_time, settings.WS_BALANCE_MAX_AGE_SECONDS):
            # Read from ws.
            result = self._ws_read_balances()
            if result:
                logger.info("[%s] Read balances by WS.", tag)
                return result

        # Get by REST.
        logger.info("[%s] Read balances by REST.", tag)
        return self._rest_get_balances()

    @staticmethod
    def serialize_dict(key: str, content_dict, std_time: datetime):
        return json.dumps({key: content_dict, "timestamp": _format_datetime(std_time)})

    def run_loop(self):
        loop_count = 0
        try:
            while self.is_running:
                loop_start_time = now()
                loop_id = loop_start_time.strftime("%Y%m%d%H%M%S_") + str(loop_count)
                loop_count += 1
                logger.info(
                    "LOOP[%s] (%s) {INTERVAL: %.2f; ACCOUNT: %s}",
                    loop_id, constants.VERSION, settings.LOOP_INTERVAL_SECONDS, settings.BITMEX_ACCOUNT_ID
                )

                # Recognize our current position. This value is to be negative when we have a short position.
                position_std_time = now()
                current_position = self.read_current_position(loop_id, position_std_time)
                serialized_position = self.serialize_dict('position', current_position, position_std_time)
                logger.info("[%s] Current position is read: %s", loop_id, serialized_position)
                self.redis.set(settings.REDIS_ACCOUNT_POSITIONS_KEY, serialized_position)

                open_orders_std_time = now()
                open_orders = self.read_open_orders(loop_id, open_orders_std_time)
                open_orders_dict = self.open_orders_to_dict(open_orders)
                serialized_open_orders = self.serialize_dict('openOrders', open_orders_dict, open_orders_std_time)
                logger.info("[%s] Open orders are read: %s", loop_id, serialized_open_orders)
                self.redis.set(settings.REDIS_ACCOUNT_OPEN_ORDERS_KEY, serialized_open_orders)

                balance_std_time = now()
                balances = self.read_balances(loop_id, balance_std_time)
                serialized_balances = self.serialize_dict('balances', balances, balance_std_time)
                logger.info("[%s] Balances are read: %s", loop_id, serialized_balances)
                self.redis.set(settings.REDIS_ACCOUNT_BALANCES_KEY, serialized_balances)

                loop_elapsed_seconds = (now() - loop_start_time).total_seconds()
                logger.info("LOOP[%s] (SUMMARY) ElapsedSeconds: %.2f", loop_id, loop_elapsed_seconds)
                # TODO refreshing bitmex client.
                # TODO save to mongo.
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
        monitor = AccountMonitor()
        monitor.run_loop()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
