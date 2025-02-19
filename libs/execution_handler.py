import threading
import time
from abc import ABC, abstractmethod
import ib_async as ib
import pika
import polars as pl
from libs.config import *
from libs.rabfile import *
import logging
import json
import datetime as dt
from collections import deque

logger = logging.getLogger('autotrade.' + __name__)


class ExecutionHandlerApp(ABC):
    """
    the class implements the Application
    that acts as the trader component of
    the architecture
    """

    @abstractmethod
    def client_send_order(self):
        """
        send portfolio orders to brokers/dealers APIs
        :return:
        """

    @abstractmethod
    def client_execution_report(self):
        """
        sends an execution report to the portfolio app after
        receiving confirmation from the broker/dealer API
        :return:
        """
    @abstractmethod
    def client_order_status(self):
        """
        sends a status request for a placed order to the
        endpoint api processing it
        :return:
        """

    @abstractmethod
    def endpoint_portfolio_orders(self):
        """
        api endpoint for requests coming from the portfolio app
        :return:
        """


class AutoExecution:

    def __init__(self):

        self.trading_connections = {}
        self.rab_connections = {}
        self._connect_trading_apis()
        self._thread_tracker = {}

        self.placed_orders = {}

        self.filled_orders = {}
        self.flags = {}

        self.session_id = dt.datetime.now().strftime('%y%m%d%H%M%S')
        self.reply_to_orders = {}


        # start threads

        self._start_order_router()


    def _connect_trading_apis(self):
        self._connect_ib_api()
        self._ib_placed_orders = {}
        self._connect_db_csv()

    def _start_fill_publisher(self):

        start_event = threading.Event()
        t = threading.Thread(target=self._setup_fill_publisher, args=(start_event,))
        t.start()
        self._thread_tracker['fill_publisher'] = t

        with threading.Lock():
            while not start_event.is_set():
                continue


        self.flags['hb_fill_publisher'] = True
        t2 = threading.Thread(target=self._heartbeat_rabcon,
                              args=(self.rab_connections['fill_publisher'],
                                    self.flags['hb_fill_publisher'],
                                    'fill_publisher', ))
        t2.start()
        self._thread_tracker['hb_fill_publisher'] = t2

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

        rab_con = RabbitConnection()

        self.rab_connections['fill_publisher'] = rab_con

        _start_event.set()

    def _update_fills(self):

        if trading_router == 'ib':
            filled_orders = self._ib_update_fills()
        elif trading_router == 'csv':
            filled_orders = self._csv_update_fills()
        else:
            raise Exception(f'{trading_router} not recognized')

        if len(filled_orders) == 0:
            logger.info(f"no fills for this timestamp")
            return

        db_fill_record = pl.read_parquet(work_path
                                         + '/synthetic_server_path/auto_exec/fill_record.parquet')

        (
            pl.concat(items=[db_fill_record,
                             filled_orders.with_columns(pl.lit(self.session_id).alias('sessionId'))],
                      how='vertical', rechunk=True)
            .write_parquet(work_path
                           +'/synthetic_server_path/auto_exec/fill_record.parquet'
                           )
        )

        rab_con = self.rab_connections['fill_publisher']

        deque((self.placed_orders.pop(key) for key in filled_orders['order_itag']), maxlen=0)

        for filled_tag in filled_orders['order_itag']:

            body = {'order_itag': filled_tag,
                    'sessionId': self.session_id}
            body = json.dumps(body)
            publisher = lambda: rab_con.channel.basic_publish(
                exchange=exchange_declarations['OrderExecution']['exchange'],
                routing_key=self.reply_to_orders[filled_tag],
                body=body,
                properties=pika.BasicProperties(content_type='application/json'))

            rab_con.connection.add_callback_threadsafe(publisher)
            self.filled_orders[filled_tag] = True

        logger.info('fill events updated')


    def _connect_ib_api(self):
        ib_con = ib.IB()
        ib_con.connect(**ibg_connection_params)
        self.trading_connections['ib'] = ib_con

        logger.info(f"Connected to IB. ClientId: {ibg_connection_params['clientId']},"
                    + f"Account: {ibg_connection_params['account']}")

        open_trades = ib_con.openTrades()
        # open_trades = ib_con2.trades()
        if len(open_trades) > 0:
            self._print_ib_open_trades(open_trades)

    @staticmethod
    def _print_ib_open_trades(open_trades):
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


    def _ib_place_order(self, order_info):

        ib_con = self.trading_connections['IB']
        import ib_async as ib
        ib_con2 = ib.IB()
        ib_con2.connect(**ibg_connection_params)
        ib_con2.positions()
        ib_con2.accountValues()

        ib.util.df(ib_con2.positions()).iloc[:,:3]
        contract_object = ib.Contract(secType="STK",
                                      symbol=order_info['symbol'],
                                      exchange='SMART',
                                      currency='USD')

        order_object = ib.LimitOrder(
            action=order_info['action'],
            totalQuantity=order_info['totalQuantity'],
            lmtPrice=order_info['lmtPrice'],
            **ib_order_kwargs)

        contract_object = ib.Contract(secType='STK',
                                      symbol='SPY',
                                      exchange='SMART',
                                      currency='USD')


        order_object = ib.LimitOrder(action='SELL',
                                     totalQuantity=3,
                                     lmtPrice=610,
                                     **ib_order_kwargs)

        # order_object.lmtPrice = 615
        trade = ib_con2.placeOrder(contract_object, order_object)
        trade = ib_con.placeOrder(contract_object, order_object)

        self.placed_orders[order_info['order_itag']] = {'api_internal_tag': trade.order.orderId,
                                                        'api_client': 'IB',
                                                        'api_clientId': ib_con.client.clientId,
                                                        'secType': order_info['secType'],
                                                        'orderType': order_info['orderType'],
                                                        'side': order_info['action'],
                                                        'totalQty': order_info['totalQuantity'],
                                                        'order_itag': order_info['order_itag'],
                                                        'placed_time_stamp': dt.datetime.now()}


        logger.info(f"order_{order_info['order_itag']} placed via IB")

        cancel_order = ib_con2.cancelOrder(b[0].order)

        a = ib_con2.trades()
        b = ib_con2.openTrades()
        ib.util.df(  ib_con2.fills() )


        ib_con2.fills()[0].execution
        c = ib_con2.reqAllOpenOrders()
        order_object = b[0].order
        ib_con2.fills()

        xd = ib.util.df(b)

        ib_con2.positions()

        ib_con2.disconnect()

    def _ib_update_fills(self):

        fills_fields = ['time', 'symbol', 'acctNumber', 'exchange',
                        'side', 'shares', 'price', 'permId',
                        'clientId',	'orderId', 'cumQty',
                        'avgPrice', 'commission']

        fills = ib.util.df(ib_con2.fills())
        fills = ib.util.df(self.trading_connections['IB'].fills())

        fills = ( [ fills[['time']] ]
                 + [ib.util.df(fills[c].to_list())
                    for c in
                    ['contract', 'execution', 'commissionReport']
                 ]
        )
        from pandas import concat
        fills = concat(fills, axis='columns')[fills_fields]
        fills = (
            pl.from_pandas(fills.loc[:, ~fills.columns.duplicated('last')])
            .with_columns(
                pl.col('time').dt.convert_time_zone('America/Lima')
                .dt.replace_time_zone(None)
                .dt.cast_time_unit('ms'))
            .rename({'orderId': 'api_internal_tag',
                     'clientId': 'api_clientId',
                     'cumQty': 'totalQty',
                     'time': 'fill_time_stamp'})
            # .select('clientId', 'totalQty', 'api_internal_tag',
            #         'avgPrice', 'commission', 'time')
            .select(pl.all().exclude('side', 'permId',
                                     'acctNumber', 'shares',
                                     'price', 'exchange'))
        )

        placed_orders = (
            pl.DataFrame(data=self.placed_orders.values(),
                         schema_overrides={'totalQty': pl.Float64})
            .filter(pl.col('client') == 'IB')
        )
        placed_orders2 = (
            pl.DataFrame(data={'internal_tag': 23, 'clientId': 7, 'totalQty': 23},
                         schema_overrides={'totalQty': pl.Float64})
        )

        filled_orders = placed_orders.join(
            other=fills,
            on=['api_internal_tag', 'api_clientId', 'totalQty'],
            how='inner',
            coalesce=True
        )

        if len(filled_orders) == 0:
            logger.debug('no trades filled in IB')
            return []

        return [filled_orders]


    def _db_csv_place_order(self, order_info):
        raise NotImplementedError

    def _csv_update_fills(self):
        return []

    def _connect_db_csv(self):
        pass

    def place_order(self, order_info):
        if trading_router == 'ib':
            self._ib_place_order(order_info)
        elif trading_router == 'csv':
            self._db_csv_place_order(order_info)
        else:
            raise Exception('trading_router not recognized')

        return trading_router
    def _start_autoexecuton_rpc_server(self):

        connection_event = threading.Event()
        t = threading.Thread(target=self._setup_autoexecution_rpc_server,
                             args=(connection_event,))

        self._thread_tracker['order_router'] = t
        t.start()

        with threading.Lock:
            while not connection_event.is_set():
                pass
        logger.info('autoexecution rpc server started')

    def _setup_autoexecution_rpc_server(self,
                                        _connection_event):

        rab_con = RabbitConnection()
        self.rab_connections['order_router'] = rab_con
        rab_con.channel.exchange_declare(**exchange_declarations['OrderExecution'])

        queue_declare = rab_con.channel.queue_declare(
            **queue_declarations['AutoExecution_server']
        )

        queue_declare = queue_declare.method.queue

        rab_con.channel.queue_bind(queue=queue_declare,
                                   routing_key=all_routing_keys['AutoExecution_server'])

        rab_con.channel.basic_consume(queue=queue_declare,
                                      on_message_callback=self._autoexecution_rpc_server_cllbck,
                                      exclusive=True,
                                      auto_ack=False)

        _connection_event.set()
        rab_con.channel.start_consuming()

    def _autoexecution_rpc_server_cllbck(self, ch, method, properties, body):

        pika.BasicProperties(content_type='application/json')

        if properties.content_type != 'application/json':
            logger.error(f"order sent to AutoExecution server not processed")
            ch.basic_nack(method.delivery_tag)
            return

        body = json.loads(body)
        api = self.place_order(body)

        print(f"order_{body['order_itag']} placed succesfully via api {api}")

        self.reply_to_orders[body['order_itag']] = properties.reply_to








# i just want you to take a moment to remember that ten years ago
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


import ib_async as ib

ib_con = ib.IB()
ib_con.connect(
    port=4001,
    clientId=0
)

ib_con.disconnect()

ib_con.Al
ib.Contract

ib.Option

a = ib_con.reqAllOpenOrders()

a[0].order.orderId = 67
for i in a:
    # print(i.orderStatus.status)
    ib_con.cancelOrder(i.order)


ib_con.disconnect()

contract_object = ib.Stock(symbol="QQQ",
                           exchange="SMART",
                           currency="USD")

# trades_creados = []


other_kwargs = {'tif': "DAY",
                "account": "U9765800",
                "clearingIntent": "IB"}


order_object = ib.Order(orderType="LMT",
                        action="BUY",
                        totalQuantity=1,
                        lmtPrice=490,
                        **other_kwargs)


    # trades_creados.append(ib_con.placeOrder(contract=contract_object,
    #                   order=order_object))


trade_object = ib_con.placeOrder(contract=contract_object,
                  order=order_object)

trade_object.orderStatus.status

open_orders = ib_con.openOrders()

order_from_phone = open_orders[0]
order_from_phone.totalQuantity = 10

ib_con.placeOrder(contract_object, order_from_phone)

open_orders[1]
a = ib_con.reqAllOpenOrders()

for i in a:
    ib_con.cancelOrder(i.order)






