import logging
from abc import ABC, abstractmethod
import polars as pl
# import pika
import time

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


class DataHandlerCSV:
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
                        pulse: int,
                        data_type_to_consume: list = None,
                        ):

        # Create Connection
        from libs.rabs.rabfile import RabbitConCSV

        rabcon = RabbitConCSV()

        data_type_to_consume = data_type_to_consume or self.consumable_data.keys()

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

                time.sleep(pulse)

                logger.debug(f"sent_data {ts.strftime('%c')}")

            return sent_data

    # def client_datafeed_receive(self):
    #
    # def portfolio_connection(self):

# class DataStorer:
#
#     def __int__(self):





