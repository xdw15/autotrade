import threading
from abc import ABC, abstractmethod
import ib_async as ib
import pika

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

    def _connect_ib_api(self):

        ib_con = ib.IB()
        ib_con.connect(port=ibg_params['socket_port'],
                       clientId=ibg_params['master_api_client_id'])

        self.trading_api_connections['ib'] = ib_con

        order_object2 = ib.Order(orderType="LMT",
                                action="BUY",
                                totalQuantity=1,
                                lmtPrice=490,
                                outsideRth=True)

        contract_object = ib.Stock(symbol='QQQ',
                                   exchange="SMART",
                                   currency="USD")
        ib.Contract()
        trade_object2 = ib_con.placeOrder(contract=contract_object,
                                         order=order_object2)

        trade_object2.isActive()
        ib_con.openOrders()
        ib_con.cancelOrder(order_object2)
        ib_con.disconnect()


    def _start_order_router(self):

        start_event = threading.Event()
        t = threading.Thread(target=self._setup_start_order_router,
                             args=(start_event,))

        self._thread_tracker['order_router'] = t
        t.start()
        logger.info('order_router started')

    def _setup_start_order_router(self,
                                  start_event):

        rab_con = RabbitConnection()
        self.rab_connections['order_router'] = rab_con
        start_event.set()
        rab_con.channel.exchange_declare(**exchange_declarations['orders'])

        queue_declare = rab_con.channel.queue_declare(queue='rpc_order_router',
                                                      passive=False,
                                                      auto_delete=True,
                                                      exclusive=True,
                                                      arguments={'x-consumer-timeout': 1*60_000})

        queue_declare = queue_declare.method.queue

        rab_con.channel.queue_bind(queue=queue_declare,
                                   routing_key=all_routing_keys['order_consumer'])

        rab_con.channel.basic_consume(queue=queue_declare,
                                      on_message_callback=self._order_router_cllbck,
                                      exclusive=True,
                                      auto_ack=False)

        rab_con.channel.start_consuming()

    def _order_router_cllbck(self, ch, method, properties, body):

        pika.BasicProperties(content_type='application/json')

        if properties.content_type != 'application/json':
            logger.error(f"order not processed")
            ch.basic_ack(method.delivery_tag)
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






