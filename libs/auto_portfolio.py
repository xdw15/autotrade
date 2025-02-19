import threading
import queue
import typing
import numpy as np
import polars as pl
from abc import ABC, abstractmethod
import pika
import datetime as dt
import json
from collections import deque
from libs.risk_manager import *

#from charset_normalizer.md import getLogger
from libs.config import *
from libs.rabfile import RabbitConnection
import logging

logger = logging.getLogger('autotrade.' + __name__)


class AutoPort(ABC):

    """
    auto port acts as a single application to
    represent an actual self trading portfolio.
    it relies on other applications to support
    its methods
    """

    @abstractmethod
    def riskmanager(self):
        """
        the 7uck1ng brain behind managing the system
        :return:
        """
        pass

    # the following methods can be used withing the scope of the application
    @abstractmethod
    def _init_composition(self) -> dict:
        """
        initialize the portfolio positions to match the app framework
        :return:
        """
        pass

    @abstractmethod
    def composition(self):
        """
        returns the composition of the portfolio
        :return:
        """
        pass

    @abstractmethod
    def trade_blot(self):
        """
        process all trading order - related information
        :return:
        """
        pass

    # these methods rely on communication with other applications

    @abstractmethod
    def client_datahandler(self):
        """
        establishes connection to interact with the datahandler app
        :return:
        """
        pass

    @abstractmethod
    def client_executionhandler(self):
        """
        establishes connection with the execuionhandler app
        :return:
        """
        pass

    @abstractmethod
    def endpoint_strategies(self):
        """
        process requests coming from strategy apps
        :return:
        """
        pass

    # these methods offer calculations for reporting/monitoring purposes

    @abstractmethod
    def report_pnl(self):
        pass


class ToyPortfolio:
    """
    a portfolio implementation that can deal with equities at first
    """

    def __init__(self,
                 initial_holdings: dict,
                 time_stamp: str,
                 price_ccy: str = None):

        time_stamp = dt.datetime.strptime(time_stamp, '%Y%m%d%H%M%S')

        self.positions = self._init_composition(
            initial_holdings,
            time_stamp
        )

        self.price_ccy = price_ccy or 'USD'

        self.shutdown_event = {}
        self.thread_tracker = {}
        self.rab_connections = {}
        self.order_tracker = {}
        self.order_counter = 0
        self.flags = {}
        # <editor-fold desc="instance updatables">
        self.mtm = None
        self.pnl = None
        # </editor-fold>

        # <editor-fold desc="instance queue container">
        self.queues = {}
        # </editor-fold>

        self._start_risk_manager()

    @staticmethod
    def _init_composition(p0: dict,
                          time_stamp: dt.datetime) -> dict:
        """
        :param p0: a dict with each key containing
        a type of security with positions vector
        :return:
        """

        # supported securities and required fields per security
        supported_securities = {
            'equity': {
                'ccy': pl.String,
                'amount': pl.Float64,
                'cost_basis': pl.Float64,
                'ticker': pl.String, },
            'cash': {
                'ccy': pl.String,
                'amount': pl.Float64,
                'cost_basis': pl.Float64, }
        }

        # ensuring the securities provided are supported
        if not all([True if input_security in supported_securities
                    else False
                    for input_security in p0.keys()]):
            raise Exception('Input security type is not currently supported')

        # ensuring the given securities have the necessary fields and dtypes
        for security in supported_securities.keys():
            present_fields = [
                True if (input_security,
                         input_fields) in supported_securities[security].items()
                else False
                for input_security, input_fields in zip(p0[security].columns,
                                                        p0[security].dtypes)
            ]

            if not all(present_fields):
                missing_fields = [
                    col for col, present in zip(p0[security].columns,
                                                present_fields)
                    if not present
                ]
                raise Exception(
                    f'''
                        Missing/non-conforming fields for security: {security} \n
                        {missing_fields}
                    '''
                )

        # returning output
        positions = {}
        for security in supported_securities.keys():
            positions[security] = (
                p0[security]
                .with_columns(
                    pl.lit(time_stamp).alias('date')
                )
                .select(['date'] + p0[security].columns)
            )

        return positions

    def portfolio_process(self,
                          mode: str = 'auto',
                          pulse_frequency: int = None):

        if mode == 'auto':
            self.shutdown_event['portfolio_process'] = threading.Event()
        elif mode == 'manual':
            pass
        else:
            raise Exception(f"mode: {mode} not valid")

    def update_system(self):

        data_event = self.queues['db_endpoint'].get()

        self._update_mtm(data_event['data'])

    def _update_pnl(self):
        pass

    def _update_mtm(self,
                    new_time_stamp,
                    data):

        if set(data.columns) != {'date', 'ticker', 'price'}:
            logger.error('columns passed to update_mtm are not conforming')
            return

        position_last_time_stamp = self.positions['equity']['date'].max()
        positions_last_amount = (
            self.positions['equity']
            .filter(pl.col('date') == position_last_time_stamp)
            .select('date', 'amount', 'ccy', 'ticker')
        )




        if self.mtm is None:
            self.mtm = (
                self.positions['equity']
                .select(pl.all().exclude('cost_basis', 'ccy'))
                .with_columns(
                    (pl.col('amount') * pl.col('price')).alias('mtm')
                )
            )

        else:
            df_0 = (
                self.positions['equity']
                .filter(pl.col('date') == last_time_stamp)
                .select(pl.all().exclude('cost_basis', 'ccy'))
                .with_columns(
                    (pl.col('amount') * pl.col('price')).alias('mtm')
                )
            )

            self.mtm = pl.concat(
                items=[self.mtm, df_0], how='vertical', rechunk=True
            )

    def connect_db_endpoint(self):

        exchange = 'exchange_data_handler',
        rt = all_routing_keys['AutoPort_db_endpoint']
        connection_event = threading.Event()
        t = threading.Thread(target=self._setup_db_endpoint,
                             args=(exchange,
                                   rt,
                                   connection_event,))

        self.thread_tracker['endpoint_data_handler'] = t
        t.start()
        self.queues['db_endpoint'] = queue.Queue()

    def close_db_endpoint(self):

        id_endpoint = 'endpoint_data_handler'
        self.rab_connections[id_endpoint].stop_plus_close()

        if self.thread_tracker[id_endpoint].is_alive():
            logger.warning('the db endpoint is still alive')
        else:
            logger.info('db endpoint closed succesfully')

    def _setup_db_endpoint(self,
                           exchange: str,
                           data_routing_keys: list,
                           connection_event: threading.Event):

        exchange = exchange or 'exchange_data_handler'
        rab_con = RabbitConnection()
        self.rab_connections['endpoint_data_handler'] = rab_con

        rab_con.channel.exchange_declare(exchange=exchange,
                                         exchange_type='topic',
                                         passive=False)

        queue_declare = rab_con.channel.queue_declare(queue='',
                                                      exclusive=True,
                                                      auto_delete=True,
                                                      passive=False)

        queue_declare = queue_declare.method.queue

        deque((rab_con.channel.queue_bind(queue=queue_declare,
                                          exchange=exchange,
                                          routing_key=rt)
               for rt in data_routing_keys),
              maxlen=0)

        def db_cllbck(ch, method, properties, body):

            db_directory = {
                'equity': db_path + '/us_equity.parquet'
            }

            if properties.content_type != 'application/json':
                logger.warning('content type not json for message sent to db client')
                logger.warning('event not processed')
                ch.basic_nack(method.delivery_tag)
                return

            ch.basic_ack(method.delivery_tag)

            msg = json.loads(body)
            msg_security_type = msg['security']['type']
            msg_securities = body['security']['tickers']
            msg_time_stamp = dt.datetime.strptime(body['time_stamp'], '%Y%m%d%H%M%S')
            positions_last = (
                self.positions[msg_security_type]
                .filter(pl.col('date') == pl.col('date').max())
            )
            positions_last_time_stamp = positions_last['date'][0]

            if not ((body['event'] == 'new_data') and (msg_time_stamp > positions_last_time_stamp)):
                logger.warning(f'event new_data has a time stamp older than last position')
                logger.warning(f'event not processed')
                return

            payload_fields = {sec: True if sec in msg_securities
            else False
                              for sec in positions_last['ticker']}

            if not all(payload_fields.values()):
                logger.warning('the following tickers are not present in the data sent')
                logger.warning(f'{[sec for sec, val in payload_fields.items() if val==False ]}')
                logger.warning('event not processed')
                return

            df = (
                pl.scan_parquet(db_directory[msg_security_type])
                .filter(
                    (pl.col('date') == msg_time_stamp)
                    & (pl.col('ticker').is_in(msg_securities))
                )
                .collect()
            )

            self.queues['db_endpoint'].put({'data': df,
                                            'time_stamp': msg_time_stamp})

        rab_con.channel.basic_consume(queue=queue_declare,
                                      on_message_callback=db_cllbck,
                                      auto_ack=False)
        connection_event.set()
        rab_con.channel.start_consuming()

    def _setup_db_rpc_client(self):

        name_exchange = 'exhange_data_handler_rpc'
        rab_con = RabbitConnection()
        rab_con.channel.exchange_declare(
            exchange=name_exchange,
            exchange_type='direct',
        )

        queue_declare = rab_con.channel.queue_declare(queue='',
                                                      passive=False,
                                                      exclusive=True)
        queue_declare = queue_declare.method.queue

        rt_key = f"rt_{queue_declare}"

        rab_con.channel.queue_bind(queue=queue_declare,
                                   exchange=name_exchange,
                                   routing_key=rt_key)

        def db_rpc_cllbck(ch, method, properties, body):
            pass

        rab_con.channel.basic_consume(queue=queue_declare,
                                      on_message_callback=db_rpc_cllbck)

        body_to_rpc = {'permission': 'acquire'}

        rab_con.channel.basic_publish(exchange=name_exchange,
                                      routing_key='rt_rpc_data_handler',
                                      body=json.dumps(body_to_rpc),
                                      properties=pika.BasicProperties(
                                          content_type='application/json',
                                          reply_to=queue_declare,
                                          # correlation_id=
                                      ))

        raise NotImplementedError

    def _start_autoexecution_rpc_client(self):

        connection_event = threading.Event()
        t = threading.Event(target=self._setup_autoexecution_rpc_client,
                            args=(connection_event, ))

        self.thread_tracker['AutoExecution_rpc'] = t

        t.start()

        with threading.Lock():
            while not connection_event.is_set():
                continue

        self.flags['hb_autoexecution_client'] = True

        t2 = threading.Thread(
            target=self._heartbeat_rabcon,
            args=(self.rab_connections['AutoExecution_client'],
                  self.flags['hb_autoexecution_client'],
                  'autoexecution_client'), )
        t2.start()
        self.thread_tracker['hb_AutoExecution_client'] = t2

        logger.info('autoexecution_rpc_client started')


    @staticmethod
    def _heartbeat_rabcon(con, flag, name, time_limit=1):

        logger.debug(f"{name} heartbeat started")
        while flag:
            con.connection.process_data_events(time_limit=time_limit)
        logger.debug(f"{name} heartbeat finished")

    def _setup_autoexecution_rpc_client(self, _connection_event):

        rab_con = RabbitConnection()
        self.rab_connections['AutoExecution_client'] = rab_con
        rab_con.channel.exchange_declare(**exchange_declarations['OrderExecution'])

        queue_declare = rab_con.channel.queue_declare(queue='',
                                                      passive=False,
                                                      auto_delete=True)

        queue_declare = queue_declare.method.queue
        rt_key = all_routing_keys['AutoExecution_client']
        rab_con.channel.queue_bind(queue=queue_declare,
                                   exchange=exchange_declarations['OrderExecution']['exchange'],
                                   routing_key=rt_key)

        def _callback_autoexecution(ch, method, properties, body):
            if properties.content_type != 'application/json':
                ch.basic_nack(method.delivery_tag)
                logger.info('AutoExecution callback msg not processed')
                return

            body = json.loads(body)



        _connection_event.set()
        rab_con.channel.basic_consume(queue=queue_declare,
                                      on_message_callback=_callback_autoexecution,
                                      auto_ack=False)



    def _start_order_receiver_endpoint(self):

        connection_event = threading.Event()
        t = threading.Thread(target=self._setup_order_receiver_endpoint,
                             args=(connection_event, ))

        self.thread_tracker['OrderReceiver'] = t
        t.start()

        with threading.Lock():
            while not connection_event.is_set():
                pass
        logger.info('OrderReceiver endpoint started')

    def _setup_order_receiver_endpoint(self, connection_event):

        rab_con = RabbitConnection()
        self.rab_connections['OrderReceiver'] = rab_con
        connection_event.set()
        rab_con.channel.exchange_declare(**exchange_declarations["OrderReceiver"])
        queue_declare = rab_con.channel.queue_declare(**queue_declarations['OrderReceiver'])

        queue_declare = queue_declare.method.queue

        deque(
            (rab_con.channel.queue_bind(queue=queue_declare,
                                        exchange=exchange_declarations['OrderReceiver'],
                                        routing_key=routing_key)
             for routing_key in all_routing_keys['AutoPort_OrderReceiver'].values()),
            maxlen=0)

        rab_con.channel.basic_consume(queue=queue_declare,
                                      on_message_callback=self._order_receiver_cllbck,
                                      exclusive=True,
                                      auto_ack=False)
        rab_con.channel.start_consuming()

    def _order_receiver_cllbck(self, ch, method, properties, body):

        if properties.content_type != 'application/json':
            logger.warning("an order sent to this application was not processed")
            ch.basic_nack(method.delivery_tag)
            return

        # the order is parsed and passed to the risk_manager app for confirmation
        body = json.loads(body)
        self.order_counter += 1
        body['order_itag'] = (body['time_stamp'][2:]
                              + f'{self.order_counter:->5}')

        t = threading.Thread(target=self.risk_manager.confirm_trade,
                             args=(body, ))
        self.order_tracker[body['order_itag']] = {'thread': t}
        t.start()
        self.debug(f"order_{body['order_itag']} passed to risk_manager for confirmation")

        ch.basic_ack(method.delivery_tag)

    def _start_risk_manager(self):
        self.risk_manager = AutoRiskManager(self)


