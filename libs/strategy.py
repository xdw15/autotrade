from abc import ABC,abstractmethod
import datetime as dt
from libs.config import work_path
import polars as pl
from libs.rabfile import *
import threading


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
                 calibration_date = dt.date,
                 ticker = str,
                 ):

        df = pl.read_parquet(work_path + '/synthetic_server_path/us_equity.parquet')
        self.day_ma = (
            df
            .filter(
                (pl.col('date').dt.date() == calibration_date)
                & ( pl.col('Ticker') == ticker )
            )
            ['price']
            .mean()
        )

        self.queue_names = {}
        self.ticker = ticker
        self.kill_switch = {}

    def endpoint_data_handler(self,
                              exchange: str,
                              routing_key: str = 'data_csv.Equity'
                              ):

        threading.Thread(
            target=self._endpoint_data_handler,
            args=(exchange,
                  routing_key,
            )
        ).start()


    def _endpoint_data_handler(self,
                               exchange: str,
                               routing_key: str,
                               ):

        rab_con = RabbitConnection()
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

        def consumer_killer():
            rab_con.connection.add_callback_threadsafe(
                rab_con.connection.close
            )

            print(f'the connection was closed: {rab_con.connection.is_closed=}')

        self.kill_switch['data_handler'] = consumer_killer
        rab_con.channel.start_consuming()

    def rabbitmq_callback(self,channel, method, properties, body):


        #body = '20250131093220'
        time_stamp = dt.datetime.strptime(
            body.decode('ASCII').split('-')[-1],
            '%Y%m%d%H%M%S')

        df = pl.read_parquet(work_path + '/synthetic_server_path/us_equity.parquet')

        current_price = (
            df
            .filter(
                (pl.col('date') == time_stamp)
                & (pl.col('Ticker') == self.ticker )
            )['price'][0]
        )

        if current_price > self.day_ma:
            print(f"with timestamp {time_stamp.strftime('%c')} sell {self.ticker} @ {current_price}")
        elif current_price < self.day_ma:
            print(f"with timestamp {time_stamp.strftime('%c')} buy {self.ticker} @ {current_price}")








