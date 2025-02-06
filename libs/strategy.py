import time
from abc import ABC, abstractmethod
import datetime as dt

import pika
import json
from libs.config import work_path
import polars as pl
from libs.rabfile import *
import threading
import logging

logger = logging.getLogger('autotrade.' + __name__)


class Strategy(ABC):
    """
    this class is intended to function as an app
    directly connected to the portfolio app
    """

    @abstractmethod
    def client_signal_postman(self):
        """
        sends signals to the portfolio app
        :return:
        """
        pass

    @abstractmethod
    def endpoint_datahandler(self):
        """
        connects to the data handler app
        :return:
        """
        pass


class DumbStrat:
    def __init__(self,
                 calibration_date=dt.date,
                 ticker=str,
                 ):

        df = pl.read_parquet(work_path + '/synthetic_server_path/us_equity.parquet')
        self.day_ma = (
            df
            .filter(
                (pl.col('date').dt.date() == calibration_date)
                & (pl.col('Ticker') == ticker)
            )
            ['price']
            .mean()
        )

        self.queue_names = {}
        self.ticker = ticker
        self.rab_connections = {}

    def connect_db_endpoint(self,
                              exchange: str,
                              routing_key: str = 'data_csv.Equity'
                              ):

        connection_event = threading.Event()
        threading.Thread(
            target=self._setup_db_endpoint,
            args=(exchange,
                  routing_key,
                  connection_event
                  )
        ).start()

        with threading.Lock():
            while not connection_event.is_set():
                pass

        logger.debug('consumer started')

    def close_db_endpoint(self):
            if self.rab_connections['data_handler'].is_open:
                self.rab_connections['data_handler'].add_callback_threadsafe(
                    self.rab_connections['data_handler'].close
                )
            else:
                logger.debug('db_endpoint connection already closed')

    def _setup_db_endpoint(self,
                               exchange: str,
                               routing_key: str,
                               connection_event: threading.Event
                               ):

        rab_con = RabbitConnection()
        self.rab_connections['data_handler'] = rab_con.connection
        connection_event.set()
        self.queue_names['data_handler'] = f"Strat-{self.__class__.__name__}-{threading.get_ident()}"
        rab_con.channel.queue_declare(
            queue=self.queue_names['data_handler'],
            passive=False,
            durable=False,
            exclusive=True,
            auto_delete=True
        )

        rab_con.channel.queue_bind(
            queue=self.queue_names['data_handler'],
            exchange=exchange,
            routing_key=routing_key
        )

        rab_con.channel.basic_consume(
            queue=self.queue_names['data_handler'],
            on_message_callback=self.rabbitmq_callback
        )

        # def consumer_killer():
        #     rab_con.connection.add_callback_threadsafe(
        #         rab_con.connection.close
        #     )

        rab_con.channel.start_consuming()
        logger.debug(f'{rab_con.connection.is_closed=}')

    def rabbitmq_callback(self, channel, method, properties, body):

        if properties.content_type == 'application/json':
            body = json.loads(body)

            # ####remove later
            from time import perf_counter
            t0 = perf_counter()
            dd1 = [cadena_de_1 + 'dd' if isinstance(cadena, str)
                   else 'jaja'
                   if len(cadena_de_1) == 1
                   else 'jajanested'
                   for cadena in body['large_shit']
                   for cadena_de_1 in cadena.split()[0]]
            dd = pl.DataFrame(body['large_shit'])
            logger.info(f'processed {len(dd1)} strings, took {perf_counter()-t0:,.4f}')
            del dd, t0, dd1
            # #####

            if (body['security']['type'] == 'Equity') \
                    and (self.ticker in body['security']['tickers']):

                time_stamp = dt.datetime.strptime(body['time_stamp'], '%Y%m%d%H%M%S')

                df = pl.read_parquet(work_path + '/synthetic_server_path/us_equity.parquet')

                current_price = (
                    df
                    .filter(
                        (pl.col('date') == time_stamp)
                        & (pl.col('Ticker') == self.ticker)
                    )['price'][0]
                )

                if current_price > self.day_ma:
                    logger.info(f"with timestamp {time_stamp.strftime('%c')} sell {self.ticker} @ {current_price}")
                elif current_price < self.day_ma:
                    logger.info(f"with timestamp {time_stamp.strftime('%c')} buy {self.ticker} @ {current_price}")

            channel.basic_ack(method.delivery_tag)

        else:
            channel.basic_nack(method.delivery_tag)
            print(f'content_type is not application/json, message not consumed')
            logger.warning(f'content_type is not application/json, message not consumed')
