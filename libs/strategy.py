import time
from abc import ABC, abstractmethod
import datetime as dt

import json
import polars as pl
from libs.config import *
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
                 calibration_date: dt.date,
                 ticker: str,
                 signal_frequency: dt.timedelta):

        df = pl.read_parquet(work_path + '/synthetic_server_path/us_equity.parquet')
        self.day_ma = (
            df
            .filter(
                (pl.col('date').dt.date() == calibration_date)
                & (pl.col('ticker') == ticker)
            )
            ['price']
            .mean()
        )

        self.queue_names = {}
        self.ticker = ticker
        self.rab_connections = {}
        self.signal_frequency = signal_frequency or dt.timedelta(seconds=30)
        self.last_signal_time_stamp = dt.datetime.fromtimestamp(0)

    def connect_db_endpoint(self,
                            exchange: str,
                            routing_key: str = 'data_csv.Equity'):

        connection_event = threading.Event()
        threading.Thread(
            target=self._setup_db_endpoint,
            args=(exchange,
                  routing_key,
                  connection_event)
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

        rab_con.channel.exchange_declare(**exchange_declarations['OrderReceiver'])
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
            on_message_callback=self.db_endpoint_callback
        )

        # def consumer_killer():
        #     rab_con.connection.add_callback_threadsafe(
        #         rab_con.connection.close
        #     )

        rab_con.channel.start_consuming()
        logger.debug(f'{rab_con.connection.is_closed=}')

    def db_endpoint_callback(self, channel, method, properties, body):

        if properties.content_type != 'application/json':
            channel.basic_nack(method.delivery_tag)
            logger.warning(f'content_type is not application/json, event not processed')
            return

        body = json.loads(body)

        # ####remove later, just testing performance
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

        time_stamp = dt.datetime.strptime(body['time_stamp'], '%Y%m%d%H%M%S')

        trigger_data_event = ((body['security']['type'] == 'equity')
                                and (self.ticker in body['security']['tickers'])
                                and (self.last_signal_time_stamp + self.signal_frequency < time_stamp))

        channel.basic_ack(method.delivery_tag)

        if not trigger_data_event:
            return

        current_price = (
            pl.scan_parquet(work_path + '/synthetic_server_path/us_equity.parquet')
            .filter(
                (pl.col('date') == time_stamp)
                & (pl.col('ticker') == self.ticker)
            ).collect()['price'][0]
        )

        signal_body = {'symbol': self.ticker,
                       'signalPrice': current_price,
                       'time_stamp': body['time_stamp'],
                       'secType': 'STK',
                       'tradeQty': 4,
                       'orderType': 'LMT',
                       # 'portAlloc': {'1': 0.9, '2': 0.1},
                       # 'typeAlloc': 'percent',
                       'portAlloc': {'1': 2, '2': 1},
                       'typeAlloc': 'amount',
        }

        if current_price > self.day_ma:
            signal_body['action'] = "SELL"
        elif current_price < self.day_ma:
            signal_body['action'] = "BUY"

        logger.info(f"Timestamp: {time_stamp.strftime('%y%m%d-%H:%M:%S')}"
                    + f"-Action: {signal_body['action']} {self.ticker} @ {current_price}")

        channel.basic_publish(
            body=json.dumps(body),
            exchange=exchange_declarations['OrderReceiver']['exchange'],
            routing_key=all_routing_keys['AutoPort_OrderReceiver']['DumbStrat'],
            properties=pika.BasicProperties(content_type='application/json',
                                            app_id='dumb_strat'))

        self.last_signal_time_stamp = time_stamp

