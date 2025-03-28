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
from libs.autoport_utils import *
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
        """
        the class is a system that manages the updating
        of the holdings (securities and cash) and calculations (pnl, mtm, etc) tables
        in the context of subapplications generating live events.
        the existing subapps are
        - db client: receives data events to update the valuations
        - autoexec client: receives fill events to update positions. and sends orders received
        - order client: receives orders from other applications to which the subaplication is subscribed

        the class portfolio should be able to invoke the same tool that would update the table
        in a non-event fashion. i.e., through manually calling a script.

        :param initial_holdings:
        :param time_stamp:
        :param price_ccy:
        """

        time_stamp = dt.datetime.strptime(time_stamp, '%Y%m%d%H%M%S')

        # self.positions = self._init_composition(
        #     initial_holdings,
        #     time_stamp
        # )

        self._init_composition()

        self.price_ccy = price_ccy or 'USD'

        self.shutdown_event = {}  # evaluate the purpose
        self.thread_tracker = {}  # create an individual dictionary for tracking the threads of each sub application
        self.rab_connections = {}
        self.order_tracker = {}
        self.order_counter = 0
        self.flags = {}
        self.lock_readwrite = threading.Lock()

        # <editor-fold desc="instance updatables">
        self.mtm = None
        self.pnl = None
        # </editor-fold>

        # <editor-fold desc="instance queue container">
        self.event_queue = {}
        # </editor-fold>

        self._start_risk_manager()

        # table handlers
        self.tables = {
            'equity': ParquetHandler(
                work_path
                + '/synthetic_server_path/auto_port/holdings/equity.parquet'),
            'cash': ParquetHandler(
                work_path
                + '/synthetic_server_path/auto_port/holdings/cash.parquet'),
            'blotLog': ParquetHandler(
                work_path
                + '/synthetic_server_path/auto_port/blotter_log.parquet'),
            'blotter': ParquetHandler(
                work_path
                + '/synthetic_server_path/auto_port/blotter.parquet'),
            'blotAlloc': ParquetHandler(
                work_path
                + '/synthetic_server_path/auto_port/blotter_alloc.parquet'),
        }

    @staticmethod
    def _init_composition(p0: dict,
                          time_stamp: dt.datetime):
        """
        :param p0: a dict with each key containing
        a type of security with positions vector
        :return:
        """
        p0 = {
            'equity': pl.read_parquet_schema(work_path + '/synthetic_server_path/auto_port/holdings/equity.parquet'),
            'cash': pl.read_parquet_schema(work_path + '/synthetic_server_path/auto_port/holdings/cash.parquet')
        }

        # supported securities and required fields per security
        supported_securities = {
            'equity': {
                # 'ccy': pl.String,
                'amount': pl.Float64,
                'cost_basis': pl.Float64,
                'ticker': pl.String,
                'port': pl.String,
                'timestamp': pl.Datetime(time_unit='ms')},
            'cash': {
                'ccy': pl.String,
                'amount': pl.Float64,
                'cost_basis': pl.Float64,
                'port': pl.String,
                'timestamp': pl.Datetime(time_unit='ms')}
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

        # # returning output
        # positions = {}
        # for security in supported_securities.keys():
        #     positions[security] = (
        #         p0[security]
        #         .with_columns(
        #             pl.lit(time_stamp).alias('date')
        #         )
        #         .select(['date'] + p0[security].columns)
        #     )
        #
        # return positions

    def _start_risk_manager(self):
        self.risk_manager = AutoRiskManager(self)

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

        data_event = self.event_queue['DH_endpoint'].get()

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

    class AutoPortDataHandlerEndpoint:

        def __init__(self):
            print('xd')
            self.connect_db_endpoint()

        def connect_db_endpoint(self):
            connection_event = threading.Event()
            self.thread = threading.Thread(
                target=self._setup_db_endpoint,
                args=(connection_event, ))

            self.thread.start()

            while not connection_event.is_set():
                continue

            logger.info('DH_endpoint started')
            self.event_queue['DH_endpoint'] = queue.Queue()

        def close_db_endpoint(self):

            id_endpoint = 'DH_endpoint'
            self.rab_connections[id_endpoint].stop_plus_close()

            if self.thread_tracker[id_endpoint].is_alive():
                logger.warning('the db endpoint is still alive')
            else:
                logger.info('db endpoint closed succesfully')

            rab_con_name = 'DH_endpoint'
            if self.rab_con.connection.is_open:

                self.rab_con.stop_plus_close()

                if self.thread.is_alive():
                    logger.warning(f'{rab_con_name} thread is still alive')
                else:
                    logger.info(f'{rab_con_name} thread closed succesfully')
            else:
                logger.info(f'{rab_con_name} connection closed already')

        def _setup_db_endpoint(self,
                               connection_event: threading.Event):

            # exchange = exchange or 'exchange_data_handler'
            self.rab_con = RabbitConnection()

            self.rab_con.channel.exchange_declare(
                **exchange_declarations['DataHandler'])

            queue_declare = self.rab_con.channel.queue_declare(
                **queue_declarations['AutoPort_DH_endpoint'])

            queue_declare = queue_declare.method.queue

            deque((self.rab_con.channel.queue_bind(
                queue=queue_declare,
                exchange=exchange_declarations['DataHandler']['exchange'],
                routing_key=rt)
                for rt in all_routing_keys['AutoPort_DH_endpoint']),
                maxlen=0)

            self.rab_con.channel.basic_consume(
                queue=queue_declare,
                on_message_callback=self.db_cllbck,
                auto_ack=False)

            connection_event.set()
            self.rab_con.channel.start_consuming()

        def db_cllbck(self, ch, method, properties, body):

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
                logger.warning(f'{[sec for sec, val in payload_fields.items() if val == False]}')
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

            self.event_queue['DH_endpoint'].put({'data': df,
                                                 'time_stamp': msg_time_stamp})

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
        t = threading.Thread(target=self._setup_autoexecution_rpc_client,
                            args=(connection_event, ))

        self.thread_tracker['AutoExecution_rpc'] = t

        t.start()

        self.event_queue['fill_confirmations'] = queue.Queue()

        with threading.Lock():
            while not connection_event.is_set():
                continue


        # starting the publisher that serves the rpc-publishing part
        self.flags['hb_autoexecution_client'] = True

        t2 = threading.Thread(
            target=self._heartbeat_rabcon,
            args=(self.rab_connections['AutoExecution_client'],
                  self.flags['hb_autoexecution_client'],
                  'autoexecution_client'), )
        t2.start()
        self.thread_tracker['hb_AutoExecution_client'] = t2

        logger.info('autoexecution_rpc_client started')

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

            # new row for blotter log
            table = self.tables['blotLog']
            new_row = {'order_itag': body['order_itag'],
                       'status': 'fill_confirmed',
                       'note': '',
                       'timestamp': dt.datetime.now()}
            overrides = {'timestamp': pl.Datetime(time_unit='ms')}

            with table.lock:
                table.update(new_row=new_row,
                             overrides=overrides)

            self.event_queue['fill_confirmations'].put(body['order_itag'])


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

        # update blotter
        table = self.tables['blotter']
        order_dic = {'timestamp': dt.datetime.now(),
                   'order_itag': body['order_itag'],
                   'secId': body['symbol'],
                   'secType': body['secType'],
                   'side': body['action'],
                   'tradeQty': body['tradeQty'],
                   'orderType': body['orderType'],
                   'orderParams': str(body['signalPrice']),
                   'placerId': properties.app_id,
                   # 'portAlloc': body['portAlloc'],
                   # 'typeAlloc': body['typeAlloc'],
        }

        overrides = {'timestamp': pl.Datetime(time_unit='ms')}

        with table.lock:
            table.update(new_row=order_dic, overrides=overrides)

        # update blotter alloc

        table = self.tables['blotAlloc']
        new_row = []
        for port_key in body['portAlloc']:
            dic_unnested = {'timestamp': order_dic['timestamp'],
                            'order_itag': order_dic['order_itag'],
                            'port': port_key,
                            'Alloc': body['portAlloc'][port_key],
                            'typeAlloc': order_dic['typeAlloc']}
            new_row.append(dic_unnested)

        with table.lock:
            table.update(new_row=new_row, overrides=overrides)

        # update blotter log
        table = self.tables['blotLog']
        new_row = {'order_itag': body['order_itag'], 'status': 'receivedByAutoPort',
                   'note': '', 'timestamp': dt.datetime.now()}
        overrides = {'timestamp': pl.Datetime(time_unit='ms')}

        with table.lock:
            table.update(new_row=new_row, overrides=overrides)

        # order dispatched to risk manager
        logger.debug(f"order_{body['order_itag']} passed to risk_manager for confirmation")
        self.risk_manager.confirm_trade(body)
        # self.order_tracker[body['order_itag']] = {'thread': t}
        # t.start()
        ch.basic_ack(method.delivery_tag)

    @staticmethod
    def _heartbeat_rabcon(con, flag, name, time_limit=1):

        logger.debug(f"{name} heartbeat started")
        while flag:
            con.connection.process_data_events(time_limit=time_limit)
        logger.debug(f"{name} heartbeat finished")

