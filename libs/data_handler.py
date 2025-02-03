import logging
from abc import ABC, abstractmethod
import polars as pl
# import pika
import time
import threading

#from ib_async import Contract
#from pyarrow import timestamp

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
            csv_info: dict,
            # pulse: int
    ):

        # checking input dictionary is compliant
        field_checker = {
            'Equity': ['Ticker', 'path', 'col_names']
        }
        for tipo_data in csv_info.keys():

            checker = [

                all([
                    True if campo in field_checker[tipo_data]
                    else False
                    for campo in csv_file

                ])
                for csv_file in csv_info[tipo_data]

            ]

            if not all(checker):
                faulty_csv = [
                    dic['Ticker']
                    for dic, ok in zip(csv_info[tipo_data], checker)
                    if not ok
                ]
                raise Exception(f'''
                    Missing fields in data type {tipo_data}\n
                    security: { faulty_csv }
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
                                pl.lit(file_dict['Ticker']).alias('Ticker')
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

        def synthetic_stream(pulse_synt):

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
                #logger.debug(f"sent_data {ts.strftime('%c')}")

                time.sleep(pulse_synt)
        return synthetic_stream

class DataHandlerPrimer:

    def __init__(self,
                 data_base_connections : dict,
                 ):

        # assume the connection is just a path where to store the parquets
        # it will check if the path exists
        from os import path as os_path

        self.db_connection = {}
        securities_supported = ['Equity']

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

        self.kill_keys = {}

    def _setup_endpoint_csv(self,
                            securities,
                            generator,
                            kill_event):

        # import pyarrow.parquet as pq

        # sche = pq.read_schema(work_path + '/synthetic_server_path/us_equity.parquet')

        from libs.rabfile import RabbitConnection

        rab_con = RabbitConnection()

        rab_con.channel.exchange_declare(
            exchange='exchange_data_handler',
            exchange_type='topic',
            passive=False,
            auto_delete=False
        )

        print('data streaming started')

        for beat in generator:
            for security in securities:

                temp_connection = pl.read_parquet(
                    self.db_connection[security]
                )

                temp_connection = pl.concat(
                    items=[temp_connection, beat[security]],
                    how='vertical'
                )

                temp_connection.write_parquet(
                    self.db_connection[security]
                )

                time_stamp = beat[security]['date'][0]
                logger.debug(f"data type {security} sent with time stamp: {time_stamp.strftime('%c')}")
                rab_con.channel.basic_publish(
                    exchange='exchange_data_handler',
                    routing_key=f'data_csv.{security}',
                    body=f"new_data-{security}-{time_stamp.strftime('%Y%m%d%H%M%S')}"
                )

            if kill_event.is_set():
                print('closing csv connection')
                break

    def setup_endpoint_csv(self,securities,generator):

        self.kill_keys['csv_endpoint'] = threading.Event()
        threading.Thread(target=self._setup_endpoint_csv,
                         args=(securities,
                               generator,
                               self.kill_keys['csv_endpoint'],
                               )
                         ).start()







