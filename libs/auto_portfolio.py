import threading

import numpy as np
import polars as pl
from abc import ABC, abstractmethod
import pika
import datetime as dt
import json

from charset_normalizer.md import getLogger

from libs.config import *
from libs.rabfile import RabbitConnection
import logging


logger = getLogger('autotrade.'+ __name__)

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

        # <editor-fold desc="updatables">
        self.mtm = None
        self.pnl = None
        # </editor-fold>

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
                'ticker': pl.String,
            },
            'cash': {
                'ccy': pl.String,
                'amount': pl.Float64,
                'cost_basis': pl.Float64,
            }
        }

        # ensuring the securities provided are supported
        if not all(
            [True if i in supported_securities else False for i in p0.keys()]
        ):
            raise Exception('Security type is not currently supported')

        # ensuring the given securities have the necessary fields
        for security in supported_securities.keys():
            present_fields = [
                True
                if (sec, fields) in supported_securities[security].items()
                else False
                for sec, fields in zip(p0[security].columns, p0[security].dtypes)
            ]

            if not all(present_fields):

                missing_fields = [
                    col for col, present in zip(
                        p0[security].columns, present_fields
                    )
                    if not present
                ]
                raise Exception(
                    f'''
                        Missing fields for security: {security} \n
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

    def update_system(self,
                      new_time_stamp):


        self._update_mtm()

    def _update_pnl(self):
        pass

    def _update_mtm(self,
                    new_time_stamp,
                    data):

        last_time_stamp = self.positions['equity']['date'][-1]

        if set(data.columns) != {'date', 'ticker'}:
            raise Exception('data passed to updated_mtm is not conforming')


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
                .filter(pl.col('date')==last_time_stamp)
                .select(pl.all().exclude('cost_basis', 'ccy'))
                .with_columns(
                    (pl.col('amount') * pl.col('price')).alias('mtm')
                )
            )

            self.mtm = pl.concat(
                items=[self.mtm, df_0], how='vertical', rechunk=True
            )


    def _setup_db_endpoint(self,
                           exchange: str,
                           routing_key: str,
                           connection_event: threading.Event):

        exchange = exchange or 'exchange_data_handler'
        routing_key = routing_key or f'data_csv.equity'
        rab_con = RabbitConnection()
        self.rab_connections['data_handler'] = rab_con
        connection_event.set()

        rab_con.channel.exchange_declare(exchange=exchange,
                                         exchange_type='topic',
                                         passive=False)

        queue_declare = rab_con.channel.queue_declare(queue='',
                                      exclusive=True,
                                      auto_delete=True,
                                      passive=False)

        queue_declare = queue_declare.method.queue

        rab_con.channel.queue_bind(queue=queue_declare,
                                   exchange=exchange,
                                   routing_key=routing_key)


        def db_cllbck(ch, method, properties, body):
            if properties.content_type != 'application/json':
                logger.error('content type not json for message sent to db client')
                raise Exception('content type not json for message sent to db client')

            body = json.loads(body)
            msg_security_type = body['security']['type']
            msg_securities = body['security']['tickers']
            msg_time_stamp = dt.datetime.strptime(body['time_stamp'], '%Y%m%d%H%M%S')
            positions_last_time_stamp = self.positions[msg_security_type]['date'][-1]

            if (body['event'] == 'new_data') and (msg_time_stamp >= positions_last_time_stamp) :

                payload_fields = {sec: True if sec in msg_securities
                                           else False
                                           for sec in self.positions[msg_security_type]['ticker']}
                if not all(payload_fields.values()):
                    logger.error('the following tickers are not present in the data sent')
                    logger.error(f'{[sec for sec, val in payload_fields.items() if val==False ]}')

                df = (
                    pl.scan_parquet(db_path + '/us_equity.parquet')
                    .filter(
                        (pl.col('date') >= positions_last_time_stamp)
                        and (pl.col('ticker').is_in())
                    )
                )

                (
                    pl.scan_parquet(db_path + '/us_equity.parquet')
                    .collect()
                    .filter(
                        pl.col('date') <= dt.datetime.now()
                    )
                )










        rab_con.channel.basic_consume(queue=queue_declare,
                                      on_message_callback=db_cllbck)

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
                                      )
        )











