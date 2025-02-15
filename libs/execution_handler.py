import threading
import time
from abc import ABC, abstractmethod
import ib_async as ib
import pika
import polars as pl
from libs.config import *
from libs.rabfile import *
import logging


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

        self.trading_api_connections = {}
        self.rab_connections = {}
        self._connect_trading_apis()
        self._thread_tracker = {}

        self._start_order_router()

    def _connect_trading_apis(self):
        self._connect_ib_api()
        self._connect_db_csv()

    def _connect_ib_api(self):
        ib_con = ib.IB()
        ib_con.connect(**ibg_connection_params)
        self.trading_api_connections['ib'] = ib_con

        logger.info(f"Connected to IB. ClientId: {ibg_connection_params['clientId']},"
                    + f"Account: {ibg_connection_params['account']}")

        open_trades = ib_con.openTrades()
        # open_trades = ib_con2.trades()
        if len(open_trades)>0:
            self._print_ib_open_trades(open_trades)

    @staticmethod
    def _print_ib_open_trades(self, open_trades):
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

        ib_con = self.trading_api_connections['ib']

        import ib_async as ib
        ib_con2 = ib.IB()
        ib_con2.connect(**ibg_connection_params)
        ib_con2.positions()
        ib_con2.accountValues()

        ib.util.df(ib_con2.accountValues())
        contract_object = ib.Stock(symbol=order_info['symbol'],
                                   exchange='SMART',
                                   currency='USD')

        order_object = ib.LimitOrder(
            action=order_info['action'],
            totalQuantity=order_info['totalQuantity'],
            lmtPrice=order_info['lmtPrice'],
            **ib_order_kwargs)

        contract_object = ib.Stock(symbol='SPY',
                                   exchange='SMART',
                                   currency='USD')


        order_object = ib.LimitOrder(action='BUY',
                                     totalQuantity=3,
                                     lmtPrice=615,
                                     **ib_order_kwargs)

        # order_object.lmtPrice = 615
        trade3 = ib_con2.placeOrder(contract_object, order_object)

        cancel_order = ib_con2.cancelOrder(b[0].order)

        a = ib_con2.reqAllOpenOrders()
        b = ib_con2.openTrades()

        order_object = b[0].order


        xd = ib.util.df(b)

        ib_con2.positions()

        ib_con2.disconnect()

    def _db_csv_place_order(self, order_info):

    def _connect_db_csv(self):
        pass

    def place_order(self, order_info):
        if trading_router == 'ib':
            self._ib_place_order(order_info)
        elif trading_router == 'csv':
            self._db_csv_place_order(order_info)

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
        _connection_event.set()
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

        rab_con.channel.start_consuming()

    def _autoexecution_rpc_server_cllbck(self, ch, method, properties, body):

        pika.BasicProperties(content_type='application/json')

        if properties.content_type != 'application/json':
            logger.error(f"order sent to rpc server not processed")
            ch.basic_nack(method.delivery_tag)
            return

        raise NotImplementedError













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






