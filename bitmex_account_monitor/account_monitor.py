import sys
import atexit
import signal

from concurrent.futures import ThreadPoolExecutor

from time import sleep
from datetime import datetime, timezone, timedelta

import json
from dateutil.parser import parse

import pymongo
import redis

from pybitmex import *
from graphitepusher import GraphiteClient

from bitmex_account_monitor.utils import log, constants
from bitmex_account_monitor.utils.settings import settings


logger = log.setup_custom_logger('account_monitor')


def now():
    return datetime.now().astimezone(constants.TIMEZONE)


def _format_datetime(dt: datetime):
    return dt.strftime("%Y-%m-%d_%H:%M:%S_%z")


def _launder_datetime_string(s: str):
    _format_datetime(parse(s))


class AccountMonitor:

    def __init__(self):
        # Client to the BitMex exchange.
        logger.info("Connecting to BitMEX exchange: %s %s" % (settings.BASE_URL, settings.SYMBOL))
        self.bitmex_client = self._create_bitmex_client()
        self.bitmex_client_refreshed_time = now()
        self.is_refreshing = False
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ws_refresh")

        # Redis client.
        logger.info("Connecting to Redis %s:%d/%d" % (settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_DB))
        self.redis = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)

        # MongoDB client.
        logger.info("Connecting to %s" % settings.MONGO_DB_URI)
        self.mongo_client = pymongo.MongoClient(settings.MONGO_DB_URI)
        self.account_monitor_db = self.mongo_client["system:account-monitor"]

        # Graphite.
        logger.info("Connecting to Graphite %s: %d", settings.GRAPHITE_HOST, settings.GRAPHITE_PORT)
        self.graphite = GraphiteClient(settings.GRAPHITE_HOST, settings.GRAPHITE_PORT)

        # Now the clients are all up.
        self.is_running = True

        """
        Once db, redis and exchange clients are created,
        register exit handler that will always release resources on any error.
        """
        atexit.register(self.exit)
        signal.signal(signal.SIGTERM, self.exit)

        self.sanity_check()

    @staticmethod
    def _create_bitmex_client():
        return BitMEXClient(
            settings.BASE_URL, settings.SYMBOL,
            api_key=settings.API_KEY, api_secret=settings.API_SECRET,
            use_websocket=True, use_rest=True,
            subscriptions=["instrument", "order", "position", "margin"],
            order_id_prefix="",
            agent_name=settings.REST_AGENT_NAME,
            http_timeout=settings.HTTP_TIMEOUT_SECONDS,
            expiration_seconds=settings.API_EXPIRATION_SECONDS
        )

    def _refresh_bitmex_client(self):
        self.bitmex_client_refreshed_time = now()
        logger.info("Now refreshing BitMEXClient.")
        self.is_refreshing = True
        self.bitmex_client.close()
        self.bitmex_client = self._create_bitmex_client()
        self.is_refreshing = False
        logger.info("Refreshed BitMEXClient.")

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

        logger.info('SHUTTING DOWN BitMEX Account Monitor. Version %s' % constants.VERSION)
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

        try:
            self.graphite.close()
        except Exception as e:
            logger.info("Unable to close Graphite client: %s", str(e))

        self.executor.shutdown(wait=False)

        # Now the clients are all down.
        self.is_running = False

        sleep(1)
        sys.exit()

    def _is_age_less_or_equal(self, table_name: str, std_time: datetime, max_allowable_seconds: float):
        count = 0
        while self.is_refreshing:
            count += 1
            if (settings.MAX_REFRESH_WAIT_SECONDS * 10) < count:
                raise Exception("BitMEX Client Refresh ERROR?")
            sleep(0.1)

        ws_updated = self.bitmex_client.get_last_ws_update(table_name)
        if not ws_updated:
            # No updates received.
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

    def _get_trade_history_of_minute(self, year, month, day, hour, minute):
        filter_obj = self.bitmex_client.create_minutely_filter(year, month, day, hour, minute)
        return self.bitmex_client.rest_get_raw_trade_history_of_account(filter_obj)

    def _get_trade_history_of_hour(self, year, month, day, hour):
        filter_obj = self.bitmex_client.create_hourly_filter(year, month, day, hour)
        return self.bitmex_client.rest_get_raw_trade_history_of_account(filter_obj)

    @staticmethod
    def _round_datetime_to_minute(dt):
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 0, 0, dt.tzinfo)

    @staticmethod
    def _round_datetime_to_hour(dt):
        return datetime(dt.year, dt.month, dt.day, dt.hour, 0, 0, 0, dt.tzinfo)

    def _save_last_trade_logged_time(self, dt, unit):
        s = dt.strftime("%Y%m%d_%H%M%S_%z")
        self.account_monitor_db["trade_logged_datetime"].update(
            {'unit': unit},
            {'unit': unit, 'datetime': s},
            upsert=True
        )
        logger.info("Last logged %s updated: %s", unit, s)

    def _load_last_trade_logged_time(self, unit):
        loaded = self.account_monitor_db["trade_logged_datetime"].find_one({'unit': unit})
        if loaded is not None:
            logger.info("Last logged %s loaded: %s", unit, loaded['datetime'])
            return datetime.strptime(loaded['datetime'], "%Y%m%d_%H%M%S_%z")
        else:
            return None

    def _save_last_trade_logged_minute(self, dt):
        self._save_last_trade_logged_time(dt, 'minute')

    def _load_last_trade_logged_minute(self):
        return self._load_last_trade_logged_time('minute')

    def _save_last_trade_logged_hour(self, dt):
        self._save_last_trade_logged_time(dt, 'hour')

    def _load_last_trade_logged_hour(self):
        return self._load_last_trade_logged_time('hour')

    def log_minutely_trades_of_account(self, std_datetime):
        tuples = []
        sleep_seconds_per_request = 2.0

        last_logged_minute = self._load_last_trade_logged_minute()
        if last_logged_minute is not None:
            target_minute = last_logged_minute + timedelta(minutes=1)
        else:
            target_minute = self._round_datetime_to_minute(std_datetime - timedelta(minutes=10))
        end_minute = std_datetime - timedelta(minutes=1)
        while target_minute <= end_minute:
            trades_of_minute = self._get_trade_history_of_minute(
                target_minute.year, target_minute.month, target_minute.day, target_minute.hour, target_minute.minute)
            logger.info("%d trades got.", len(trades_of_minute))
            buys = 0
            sells = 0
            for each_trade in trades_of_minute:
                each_timestamp = int(parse(each_trade["timestamp"]).timestamp())
                each_price = float(each_trade["price"])
                if each_trade["side"] == "Buy":
                    tuples.append(("account.trade.buy", (each_timestamp, each_price)))
                    buys += 1
                else:
                    tuples.append(("account.trade.sell", (each_timestamp, each_price)))
                    sells += 1
            minutely_unix_time = int(target_minute.timestamp())
            tuples.append(("account.trade-count.minutely.buy", (minutely_unix_time, buys)))
            tuples.append(("account.trade-count.minutely.sell", (minutely_unix_time, sells)))
            sleep(sleep_seconds_per_request)
            target_minute = target_minute + timedelta(minutes=1)
        self._save_last_trade_logged_minute(target_minute - timedelta(minutes=1))
        if 0 < len(tuples):
            logger.info("Logging %d minutely trade data.", len(tuples))
            self.graphite.batch_send_tuples(tuples)
        else:
            logger.info("No minutely trades to log.")

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
                if not self.bitmex_client.is_market_in_normal_state():
                    logger.warning("[%s] Market state error: %s", loop_id, self.bitmex_client.ws_market_state())
                    break
                if self.bitmex_client.ws_market_state() == "Closed":
                    logger.info("[%s] Market is closed.", loop_id)
                    sleep(settings.LOOP_INTERVAL_SECONDS / 2.0)
                    continue

                log_data = []
                # Recognize our current position. This value is to be negative when we have a short position.
                position_std_time = now()
                current_position = self.read_current_position(loop_id, position_std_time)
                serialized_position = self.serialize_dict('position', current_position, position_std_time)
                logger.info("[%s] Current position is read: %s", loop_id, serialized_position)
                self.redis.set(settings.REDIS_ACCOUNT_POSITIONS_KEY, serialized_position)
                logger.info("[%s] Current position written to redis.", loop_id)

                log_data.append(("account.position", current_position))

                open_orders_std_time = now()
                open_orders = self.read_open_orders(loop_id, open_orders_std_time)
                open_orders_dict = self.open_orders_to_dict(open_orders)
                serialized_open_orders = self.serialize_dict('openOrders', open_orders_dict, open_orders_std_time)
                logger.info("[%s] Open orders are read: %d", loop_id, len(open_orders.bids) + len(open_orders.asks))
                self.redis.set(settings.REDIS_ACCOUNT_OPEN_ORDERS_KEY, serialized_open_orders)
                logger.info("[%s] Open orders written to redis.", loop_id)

                log_data.append(("account.open-bids", len(open_orders.bids)))
                log_data.append(("account.open-asks", len(open_orders.asks)))

                balance_std_time = now()
                balances = self.read_balances(loop_id, balance_std_time)
                serialized_balances = self.serialize_dict('balances', balances, balance_std_time)
                logger.info("[%s] Balances are read: %s", loop_id, serialized_balances)
                self.redis.set(settings.REDIS_ACCOUNT_BALANCES_KEY, serialized_balances)
                logger.info("[%s] Balances written to redis.", loop_id)

                satoshi_to_btc = 100000000.0
                log_data.append(("account.withdrawable-btc-balance", balances["withdrawableBalance"] / satoshi_to_btc))
                log_data.append(("account.wallet-btc-balance", balances["walletBalance"] / satoshi_to_btc))

                loop_elapsed_seconds = (now() - loop_start_time).total_seconds()
                logger.info("LOOP[%s] (SUMMARY) ElapsedSeconds: %.2f", loop_id, loop_elapsed_seconds)

                log_data.append(("system.account-monitor.operation.loop-time", loop_elapsed_seconds))
                self.graphite.batch_send(log_data)

                trades_std_time = now()
                self.log_minutely_trades_of_account(trades_std_time)

                if settings.WS_REFRESH_INTERVAL < (now() - self.bitmex_client_refreshed_time).total_seconds():
                    self.executor.submit(self._refresh_bitmex_client)
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
