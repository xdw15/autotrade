import threading
import time
import ib_async as ib
import pika
import polars as pl
from libs.config import *
from libs.rabfile import *
import logging
import json
import datetime as dt
from collections import deque
from libs.autoport_utils import ParquetHandler

logger = logging.getLogger('autotrade.' + __name__)


class FillPublisher:

    def __init__(self):
        self.start_event = threading.Event()
        self.thread = threading.Thread(
            target=self._setup_fill_publisher,
            args=(self.start_event, ))
        # self._start_fill_publisher()
        self.flags_rab_con_hb = True
        self.thread_heartbeat = threading.Thread(
            target=self._heartbeat_rabcon,
            args=(self.rab_con,
                  self.flags_rab_con_hb,
                  'fill_publisher', ))

    def start(self):

        self.thread.start()

        with threading.Lock():
            while not self.start_event.is_set():
                continue

        self.thread_heartbeat.start()

        # 0ta 0ara 0r9bar
        # para probar otra vez
        logger.info('fill_publisher started')

    @staticmethod
    def _heartbeat_rabcon(con, flag, name, time_limit=1):

        logger.debug(f"{name} heartbeat started")
        while flag:
            con.connection.process_data_events(time_limit=time_limit)
        logger.debug(f"{name} heartbeat finished")

    def _setup_fill_publisher(self, _start_event):

        self.rab_con = RabbitConnection()

        _start_event.set()


class AutoExecRPC:

    def __init__(self, autoexec):
        self.autoexec = autoexec
        # self._start_autoexecuton_rpc_server()

    def start(self):

        connection_event = threading.Event()
        self.thread = threading.Thread(
            target=self._setup_autoexecution_rpc_server,
            args=(connection_event,))

        self.thread.start()

        with threading.Lock():
            while not connection_event.is_set():
                pass
        logger.info('autoexecution rpc server started')

    def _setup_autoexecution_rpc_server(self,
                                        _connection_event):

        self.rab_con = RabbitConnection()
        # self.rab_connections['order_router'] = rab_con
        self.rab_con.channel.exchange_declare(
            **exchange_declarations['OrderExecution'])

        queue_declare = self.rab_con.channel.queue_declare(
            **queue_declarations['AutoExecution_rpc_server'])

        queue_declare = queue_declare.method.queue

        self.rab_con.channel.queue_bind(
            queue=queue_declare,
            exchange=exchange_declarations['OrderExecution']['exchange'],
            routing_key=all_routing_keys['AutoExecution_server'])

        self.rab_con.channel.basic_consume(
            queue=queue_declare,
            on_message_callback=self._autoexecution_rpc_server_cllbck,
            exclusive=True,
            auto_ack=False)

        _connection_event.set()
        self.rab_con.channel.start_consuming()

    def _autoexecution_rpc_server_cllbck(self, ch, method, properties, body):

        pika.BasicProperties(content_type='application/json')

        if properties.content_type != 'application/json':
            logger.error(f"order sent to AutoExecution server not processed")
            ch.basic_nack(method.delivery_tag)
            return

        body = json.loads(body)
        body['reply_to'] = properties.reply_to

        api = self.autoexec.place_order(body)

        print(f"order_{body['order_itag']} placed succesfully via api {api}")


class AutoExecAutoUpdater:

    def __init__(self, autoexec):
        self.autoexec = autoexec
        self.switch_event = threading.Event()
        # self.start_autoupdate()

    def start(self):

        self.switch_event.clear()
        self.thread = threading.Thread(
            target=self._setup_autoupdater,
            args=())

        self.thread.start()
        # self._thread_tracker['auto_update'] = t

    def _setup_autoupdater(self):

        while not self.switch_event.is_set():
            with threading.Lock():
                self.autoexec.update_system()
                time.sleep(self.autoexec.update_freq)

        logger.info('autoupdate stopped')
        print('autoupdate stopped')

    def stop_autoupdate(self):
        self.switch_event.set()
        while self.thread.is_alive():
            pass


class IBHandler:

    def __init__(self, autoexec):
        self.autoexec = autoexec

    def connect(self):
        self.ib_con = ib.IB()
        self.ib_con.connect(**ibg_connection_params)

        logger.info(f"Connected to IB. ClientId: {ibg_connection_params['clientId']},"
                    + f"Account: {ibg_connection_params['account']}")

        open_trades = self.ib_con.openTrades()
        # open_trades = ib_con2.trades()
        if len(open_trades) > 0:
            self._print_open_trades(open_trades)

    @staticmethod
    def _print_open_trades(open_trades):
        from dateutil import tz
        show_fields = {'order': ['clientId', 'orderId',
                                 'action', 'totalQuantity',
                                 'orderType', 'lmtPrice'],
                       'contract': ['symbol'],
                       'orderStatus': ['status', 'filled', 'remaining']}

        open_trades_info = []
        for trd in open_trades:
            trd = vars(trd)
            trd_values = {}

            for fld in show_fields:
                trd_values.update({key: value
                                   for key, value in vars(trd[fld]).items()
                                   if key in show_fields[fld]})

            try:
                lastlog_time_stamp = (trd['log'][-1]
                                      .time
                                      .astimezone(tz=tz.gettz('America/Lima'))
                                      .strftime('%y%m%d-%H:%M:%S'))
            except IndexError:
                lastlog_time_stamp = ''
            trd_values.update({'lastLog': lastlog_time_stamp})
            open_trades_info.append(trd_values)

        with pl.Config(tbl_cols=sum([len(_) for _ in show_fields.values()]) + 1,
                       tbl_rows=20):
            print('The following trades were open before the connection started:')
            print(pl.DataFrame(open_trades_info))

    def place_order(self, order_info):
        # ib_con = self.trading_connections['IB']

        contract_object = ib.Contract(
            secType="STK",
            symbol=order_info['symbol'],
            exchange='SMART',
            currency='USD')

        order_object = ib.LimitOrder(
            action=order_info['action'],
            totalQuantity=order_info['totalQuantity'],
            lmtPrice=order_info['lmtPrice'],
            **ib_order_kwargs)

        trade = self.ib_con.placeOrder(contract_object, order_object)

        # pick table up

        new_order = {'sessionId': self.autoexec.session_id,
                   'api_internal_tag': trade.order.orderId,
                    'api_client': 'IB',
                    'api_clientId': self.ib_con.client.clientId,
                    'secId': order_info['symbol'],
                    'secType': order_info['secType'],
                    'orderType': order_info['orderType'],
                    'side': order_info['action'],
                    'totalQty': order_info['totalQuantity'],
                    'order_itag': order_info['order_itag'],
                    'placed_timestamp': dt.datetime.now(),
                    'status': 'open',
                    'reply_to': order_info['reply_to']}

        overrides = {
            'totalQty': pl.Float64,
            'placed_timestamp': pl.Datetime(time_unit='ms')}

        self.autoexec.orders.update(
            new_row=new_order,
            overrides=overrides)

        logger.info(f"order_{order_info['order_itag']} placed via IB")

    def update_fills(self):

        fills_fields = ['time', 'symbol', 'acctNumber', 'exchange',
                        'side', 'shares', 'price', 'permId',
                        'clientId',	'orderId', 'cumQty',
                        'shares', 'avgPrice', 'commission']

        fills = ib.util.df(self.ib_con.fills())
        if fills is None:
            return []
        fills = ([fills[['time']]]
                 + [ib.util.df(fills[c].to_list())
                    for c in
                    ['contract', 'execution', 'commissionReport']
                 ])
        from pandas import concat
        fills = concat(fills, axis='columns')[fills_fields]
        fills = (
            pl.from_pandas(fills.loc[:, ~fills.columns.duplicated('last')])
            .with_columns(
                pl.col('time').dt.convert_time_zone('America/Lima')
                .dt.replace_time_zone(None)
                .dt.cast_time_unit('ms'),
                pl.col('shares').sum().alias('totalQty'),
                pl.col('shares').alias('tradeQty'))
            .rename({'orderId': 'api_internal_tag',
                     'clientId': 'api_clientId',
                     'symbol': 'secId',
                     # 'cumQty': 'totalQty',
                     'time': 'fill_timestamp'})
            # .select('clientId', 'totalQty', 'api_internal_tag',
            #         'avgPrice', 'commission', 'time')
            .select(pl.all().exclude('side', 'permId',
                                     'acctNumber', 'shares',
                                     'price', 'exchange', 'cumQty'))
        )

        placed_orders = (
            # pl.DataFrame(data=self.placed_orders.values(),
            #              schema_overrides={'totalQty': pl.Float64})
            # pl.read_parquet(
            #     work_path
            #     + '/synthetic_server_path/auto_exec/order_record.parquet'
            # )
            self.autoexec.orders.get_lazy()
            .filter((pl.col('api_client') == 'IB')
                    & (pl.col('status') == 'open')
                    & (pl.col('sessionId') == self.autoexec.session_id))
            .drop('status', 'reply_to')
            .collect()
        )

        filled_orders = placed_orders.join(
            other=fills,
            on=['api_internal_tag', 'api_clientId', 'totalQty', 'secId'],
            how='inner',
            coalesce=True
        )

        if len(filled_orders) == 0:
            logger.debug('no trades filled in IB')
            return []

        return [filled_orders]


class CSVHandler:

    def __init__(self, autoexec):
        self.autoexec = autoexec

    def connect(self):
        raise NotImplementedError

    def place_order(self, order_info):
        raise NotImplementedError

    def update_fills(self):
        return []

    def _connect_db_csv(self):
        pass


class AutoExecution:

    def __init__(self,
                 start_auto_update=True,
                 update_freq=10):

        self.fill_publisher = FillPublisher()
        self.place_order_rpc_server = AutoExecRPC(self)
        self.auto_updater = AutoExecAutoUpdater(self)

        self.client = {'ib': IBHandler(self),
                       'csv': CSVHandler(self)}

        for api_name in allowed_trading_apis:
            if api_name not in self.client.keys():
                print(f"the api {api_name} has no associated client")
                raise Exception

        self.orders = ParquetHandler(
            work_path
            + '/synthetic_server_path/auto_exec/order_record.parquet')

        self.fills = ParquetHandler(
            work_path
            + '/synthetic_server_path/auto_exec/fill_record.parquet')

        self.session_id = dt.datetime.now().strftime('%y%m%d')

        # -----
        # self.reply_to_orders = {}
        self.start_auto_update = start_auto_update
        self.update_freq = update_freq
        # start threads
        self._start_autoexecution()

    def _start_autoexecution(self):
        self._connect_trading_apis()
        self.fill_publisher.start()
        self.place_order_rpc_server.start()
        if self.start_auto_update:
            self.auto_updater.start()

    def update_system(self):
        timestamp = dt.datetime.now()
        self._update_fills()
        print(f"AutoExec updated - {timestamp.strftime('%c')}")

    def _connect_trading_apis(self):

        for api_name in allowed_trading_apis:
            self.client[api_name].connect()

    def _update_fills(self):

        filled_orders = []
        for api_name in allowed_trading_apis:
            filled_orders.append(
                self.client[api_name].update_fills()
            )


        if len(filled_orders) == 0:
            # print('No new fills')
            logger.info(f"no fills for this timestamp")
            return

        # The program is supposed to concat multiple filled orders from several apis
        # in this case there is only one appi so no concat needed
        filled_orders = filled_orders[0]
        unique_order_itag = filled_orders['order_itag'].unique()

        # updating the db_fill and db_order
        updated_orders_db = (
            # db_order_record
            self.orders.get()
            .with_columns(
                # update status
                pl.when(pl.col('order_itag').is_in(unique_order_itag))
                .then(pl.lit('filled'))
                .otherwise(pl.col('status'))
                .alias('status'),
                # update status_timestamp
                pl.when(pl.col('order_itag').is_in(unique_order_itag))
                .then(pl.lit(dt.datetime.now()))
                .otherwise(pl.col('status_timestamp'))
                .alias('status_timestamp'))
        )

        orders_to_update = (
            updated_orders_db
            .filter(pl.col('order_itag').is_in(unique_order_itag))
            .select('order_itag', 'reply_to'))

        if len(orders_to_update) != len(unique_order_itag):
            logger.error('N of orders to update is not equal to fills')
            raise Exception('N of orders to update is not equal to fills')

        self.orders.write(new_df=updated_orders_db)

        self.fills.update(new_row=filled_orders)

        # deque((self.placed_orders.pop(key) for key in unique_order_itag), maxlen=0)

        # publishing part
        rab_con = self.fill_publisher.rab_con
        filled_orders = (
            filled_orders
            .select(
                fill_ts=pl.col('fill_timestamp').dt.strftime('%Y%m%d%H%M%S'))
        )
        for filled_tag, reply_to_tag in zip(orders_to_update['order_itag'],
                                            orders_to_update['reply_to']):

            fill_ts = (
                filled_orders
                .filter(pl.col('fill_ts').is_in([filled_tag]))
                ['fill_ts'].to_list()
            )

            body = {'order_itag': filled_tag,
                    'sessionId': self.session_id,
                    'fill_timestamps': fill_ts}
            body = json.dumps(body)

            def publisher(): return rab_con.channel.basic_publish(
                exchange=exchange_declarations['OrderExecution']['exchange'],
                routing_key=reply_to_tag,
                body=body,
                properties=pika.BasicProperties(content_type='application/json'))

            rab_con.connection.add_callback_threadsafe(publisher)

        logger.info('fill events updated')

    def place_order(self, order_info):

        api_name = self.pick_trade_venue(order_info)
        self.client[api_name].place_order(order_info)

        return api_name

    @staticmethod
    def pick_trade_venue(order):
        type(order)
        return default_api


# I just want you to take a moment to remember that ten years ago
# you sat down to take a mock college admission test. It was the
# first time that you were competing with a bigger pool of people.
# or at least the first time you were physically aware of it.
# I know that we are not down for reminiscing of the past.
# But this is differente because that Marcelo from ten years ago
# had no idea what he would be ten years forward. Nor do I know
# what will be of me ten years from now. The idea I am laying out
# is that, just like me and every other moment, Marcelo from the
# future is counting on me. Counting on me being happy now, feeling
# blessed for living the very same moments we are cheerishing from
# the past. Because these moments, are as precious as they were ten
# years in the past. They are everything, they are really worth
# enjoying, because it is us. Because as long as it is us, we will
# be happy. Just make yourself happy and everyone else happy by
# living and being aware of the present. The more aware of how blessed
# we are for the now, the more present in the now we will be.



