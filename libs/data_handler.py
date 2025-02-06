from abc import ABC, abstractmethod
import json
import pika
import polars as pl
# import pika
import time
import threading
from typing import Iterable, Generator
import queue
from libs.rabfile import *

# from ib_async import Contract
# from pyarrow import timestamp

from libs.config import work_path

logger = logging.getLogger('autotrade.' + __name__)


class DataHandlerApp(ABC):
    """
    this class serves as an application that handles connections with
    other data apis, stores the information and act as an API endpoint
    for other app clients
    """

    @abstractmethod
    def store_data(self):
        """
        stores data after a successful request to a data stream
        :return:
        """
        pass

    @abstractmethod
    def update_data(self):
        """
        allows por the possibility of updating/replacing
        data at a specific timestamp
        :return:
        """
        pass

    @abstractmethod
    def client_datafeed(self):
        """
        client for sending data feed/APIs requests
        and managing requests
        :return:
        """
        pass

    @abstractmethod
    def client_strategies(self):
        """
        send alerts to strategies that there's new data
        :return:
        """
        pass

    @abstractmethod
    def client_portfolio(self):
        """
        send alerts to the portfolio that there's new data
        :return:
        """
        pass

    @abstractmethod
    def endpoint_strategies(self):
        """
        api endpoint for data requests from strategies
        :return:
        """
        pass

    @abstractmethod
    def endpoint_portfolio(self):
        """
        api endpoint for data requests from portfolio
        :return:
        """
        pass


class DataAPICSV:
    """
    replicates a live daily trading session
    """

    def __init__(
            self,
            csv_info: dict):
        # pulse: int

        # checking input dictionary is compliant
        field_checker = {'equity': ['ticker', 'path', 'col_names']}

        for tipo_data in csv_info.keys():

            checker = [

                all([
                    True if campo in field_checker[tipo_data]
                    else False
                    for campo in csv_file])

                for csv_file in csv_info[tipo_data]

            ]

            if not all(checker):
                faulty_csv = [
                    dic['ticker']
                    for dic, ok in zip(csv_info[tipo_data], checker)
                    if not ok
                ]
                raise Exception(f'''
                    Missing fields in data type {tipo_data}\n
                    security: {faulty_csv}
                    ''')

        self._csv_info = csv_info

        logger.debug('Input dictionary has the necessary fields')

        # data is retrieved from according to given parameters

        self.consumable_data = {}
        self.time_stamps = []
        for tipo_data, csv_files in self._csv_info.items():
            self.consumable_data[tipo_data] = (
                pl.concat(
                    items=[
                        (
                            pl.read_csv(file_dict['path'])
                            .rename(
                                mapping={
                                    col_name_csv: col_name_db
                                    for col_name_db, col_name_csv
                                    in file_dict['col_names'].items()
                                }
                            )
                            .select(file_dict['col_names'].keys())
                            .with_columns(
                                pl.lit(file_dict['ticker']).alias('ticker')
                            )
                        )
                        for file_dict in csv_files
                    ],
                    how='vertical',
                    rechunk=True
                )
                .with_columns(
                    pl.col('date').str.to_datetime(
                        format='%Y-%m-%d %H:%M:%S%z',
                        time_zone='America/New_York',
                        time_unit='ms'
                    )
                    .dt.replace_time_zone(None)
                    .alias('date')
                )

            )

            self.time_stamps.append(
                self.consumable_data[tipo_data]['date'].unique()
            )

        self.time_stamps = (
            pl.concat(
                items=self.time_stamps,
                rechunk=True
            )
            .unique()
            .sort(descending=False)
        )

        logger.info('finished initialization')
        # self.consumable_data = None
        # self.time_stamps = None
        # self.client_datafeed(pulse)

        print('Connected to CSV Feed')

    def client_datafeed(self,
                        pulse: int = 1,
                        data_type_to_consume: list = None,
                        ):

        # Create Connection
        # from libs.rabfile import RabbitConCSV

        # rabcon = RabbitConCSV()

        data_type_to_consume = data_type_to_consume or self.consumable_data.keys()

        def synthetic_stream():

            for ts in self.time_stamps:

                sent_data = {}
                for tipo_data in data_type_to_consume:
                    sent_data[tipo_data] = (
                        self.consumable_data[tipo_data]
                        .filter(
                            pl.col('date') == ts
                        )
                    )

                    # rabcon.produce(
                    #     body=f'''
                    #     {ts}___{tipo_data}
                    #                 ''',
                    #     routing_key=f'data_csv.{tipo_data}'
                    # )

                yield sent_data
                # logger.debug(f"sent_data {ts.strftime('%c')}")

                time.sleep(pulse)

        return synthetic_stream()


class DataHandlerPrimer:

    def __init__(self,
                 data_base_connections: dict,
                 ):

        # <editor-fold desc="initializing the connection to the database">
        # assume the connection is just a path where to store the parquets
        # it will check if the path exists

        from os import path as os_path

        self.db_connection = {}
        securities_supported = ['equity']

        for security, connection in data_base_connections.items():

            if security in securities_supported:
                if os_path.exists(connection):
                    self.db_connection[security] = connection
                else:
                    logger.error(f'data base path not found for security: {security}')
                    raise Exception(f'data base path not found for security: {security}')
            else:
                logger.error(f"Security: '{security}' not supported")
                raise Exception(f"Security: '{security}' not supported")
        # </editor-fold>

        # <editor-fold desc="ways to shut down stuff">
        self.shutdown_event = {}

        # </editor-fold>

        # <editor-fold desc="assigning other attributes">
        self.queue_db_handler = queue.Queue()
        self.rab_connections = {}

        # </editor-fold>

    def connect_csv_endpoint(self,
                             securities: Iterable,
                             generator: Generator):

        self.shutdown_event['csv_endpoint'] = threading.Event()
        threading.Thread(target=self._setup_connect_csv_endpoint,
                         args=(securities,
                               generator,
                               )
                         ).start()

    def close_csv_endpoint(self):
        if self.shutdown_event['csv_endpoint'].is_set():
            logger.info('csv_endpoint was closed already')
        else:
            self.shutdown_event['csv_endpoint'].set()

    def _setup_connect_csv_endpoint(self,
                                    securities: Iterable,
                                    generator: Generator):
        # import pyarrow.parquet as pq

        # sche = pq.read_schema(work_path + '/synthetic_server_path/us_equity.parquet')

        logger.debug('started streaming from csv endpoint')

        for beat in generator:
            for security in securities:
                self.queue_db_handler.put(
                    {'data': beat[security],
                     'info': security}
                )
                time_stamp = beat[security]['date'][0]
                logger.debug(f"data streamed from csv_api {time_stamp}")

            if self.shutdown_event['csv_endpoint'].is_set():
                print('csv connection closed')
                logger.info('csv connection closed')
                return

    def start_db_maintainer(self):
        self.shutdown_event['db_maintainer'] = threading.Event()

        threading.Thread(
            target=self._setup_db_maintainer,
            args=()
        ).start()

    def stop_db_maintainer(self):
        if self.shutdown_event['db_maintainer'].is_set():
            logger.info('db maintainer was stopped already')
        else:
            self.shutdown_event['db_maintainer'].set()

    def _setup_db_maintainer(self):

        from libs.rabfile import RabbitConnection

        rab_con = RabbitConnection()
        self.rab_connections['handler'] = rab_con
        rab_con.channel.exchange_declare(
            exchange='exchange_data_handler',
            exchange_type='topic',
            passive=False,
            auto_delete=False
        )

        logger.debug('DataHandler process started')

        while not self.shutdown_event['db_maintainer'].is_set():

            try:
                queue_item = self.queue_db_handler.get(block=True, timeout=60)
            except queue.Empty:
                continue

            security = queue_item['info']
            data = queue_item['data']

            temp_connection = pl.read_parquet(
                self.db_connection[security]
            )

            temp_connection = pl.concat(
                items=[temp_connection, data],
                how='vertical'
            )

            temp_connection.write_parquet(
                self.db_connection[security]
            )

            import string
            time_stamp = data['date'][0]
            mensaje = {'time_stamp': time_stamp.strftime('%Y%m%d%H%M%S'),
                       'security': {'type': security,
                                    'tickers': data['ticker'].to_list()},
                       'event': 'new_data',
                       'large_shit': [string.printable] * 10000}  # delete this later

            rab_con.channel.basic_publish(
                exchange='exchange_data_handler',
                routing_key=f'data_csv.{security}',
                body=json.dumps(mensaje),
                properties=pika.BasicProperties(content_type='application/json')
            )
            logger.debug(f"data---{security}---sent: {time_stamp.strftime('%c')}")
            self.queue_db_handler.task_done()

        rab_con.connection.close()
        logger.debug('Data maintainer was shutdown')
