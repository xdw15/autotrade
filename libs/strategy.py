import time
from abc import ABC, abstractmethod
import datetime as dt

import json
import polars as pl
from libs.config import *
from libs.rabfile import *
import threading
import logging
from libs.autoport_utils import *
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

        self.df = ParquetHandler(work_path + '/synthetic_server_path/us_equity.parquet')
        df_filtered = (
            self.df.get()
            .filter(
                (pl.col('date').dt.date() == calibration_date)
                & (pl.col('ticker') == ticker))
        )

        self.day_ma = df_filtered['price'].mean()

        self.ticker = ticker
        self.signal_frequency = signal_frequency or dt.timedelta(seconds=30)
        self.last_signal_time_stamp = df_filtered['date'].min()
        self.thread = threading.Thread()

    def connect_db_endpoint(self):

        connection_event = threading.Event()
        self.thread = threading.Thread(
            target=self._setup_db_endpoint,
            args=(connection_event, )
        )

        self.thread.start()

        while not connection_event.is_set():
            pass

        logger.info('DumbStrat db_endpoint con started')

    def close_db_endpoint(self):
        if self.rab_con.connection.is_open:
            self.rab_con.stop_plus_close()
        else:
            logger.info('DumbStrat db_endpoint con already closed')

    def _setup_db_endpoint(self,
                           connection_event: threading.Event):

        self.rab_con = RabbitConnection()
        connection_event.set()

        self.rab_con.channel.exchange_declare(
            **exchange_declarations['OrderReceiver']
        )

        # self.queue_names['data_handler'] = f"Strat-{self.__class__.__name__}-{threading.get_ident()}"

        self.rab_con.channel.queue_declare(
            **queue_declarations['DumbStrat']
        )

        self.rab_con.channel.queue_bind(
            queue=queue_declarations['DumbStrat']['queue'],
            exchange=exchange_declarations['DataHandler']['exchange'],
            routing_key=all_routing_keys['DumbStrat_DH_endpoint']
        )

        self.rab_con.channel.basic_consume(
            queue=queue_declarations['DumbStrat']['queue'],
            on_message_callback=self.db_endpoint_cllbck
        )

        # def consumer_killer():
        #     rab_con.connection.add_callback_threadsafe(
        #         rab_con.connection.close
        #     )

        self.rab_con.channel.start_consuming()
        logger.info(f'{self.rab_con.connection.is_closed=}')

    def db_endpoint_cllbck(self, channel, method, properties, body):

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

        time_stamp = dt.datetime.strptime(body['timestamp'], '%Y%m%d%H%M%S')

        trigger_data_event = (
                (body['table_name'] == 'us_equity')
                and (self.ticker in body['tickers'])
                and (self.last_signal_time_stamp + self.signal_frequency < time_stamp)
        )

        channel.basic_ack(method.delivery_tag)

        if not trigger_data_event:
            return

        current_price = (
            self.df.get()
            .filter(
                (pl.col('date') == time_stamp)
                & (pl.col('ticker') == self.ticker)
            )['price'][0]
        )

        signal_body = {'symbol': self.ticker,
                       'orderType': 'LMT',
                       'lmtPrice': current_price,
                       'signal_timestamp': body['timestamp'],
                       'secType': 'STK',
                       'totalQuantity': 1,
                       # 'portAlloc': {'1': 0.9, '2': 0.1},
                       # 'typeAlloc': 'percent',
                       'portAlloc': {'1': 2, '2': 1},
                       'typeAlloc': 'amount',
        }

        if current_price > self.day_ma:
            signal_body['action'] = "BUY" # "SELL"
        elif current_price < self.day_ma:
            signal_body['action'] = "SELL"

        logger.info(f"Timestamp: {time_stamp.strftime('%y%m%d-%H:%M:%S')}"
                    + f"-Action: {signal_body['action']} {self.ticker} @ {current_price}")

        channel.basic_publish(
            body=json.dumps(signal_body),
            exchange=exchange_declarations['OrderReceiver']['exchange'],
            routing_key=all_routing_keys['AutoPort_OrderReceiver']['DumbStrat'],
            properties=pika.BasicProperties(
                content_type='application/json',
                app_id='dumb_strat')
        )

        self.last_signal_time_stamp = time_stamp

