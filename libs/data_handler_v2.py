import json
import polars as pl
import time
import threading
from typing import Iterable, Generator
import queue
from libs.rabfile import *
from libs.config import *
import datetime as dt
from libs.autoport_utils_playground import *

dict_of_equity_files = [
    {'ticker': 'DTCR',
     'path': work_path + '/archivosvarios/dta_dtcr.parquet'},
    {'ticker': 'DTCR',
     'path': work_path + '/archivosvarios/dta_dtcr250220.parquet'},
    {'ticker': 'AAPL',
     'path': work_path + '/archivosvarios/AAPL.csv'},
    {'ticker': 'AAPL',
     'path': work_path + '/archivosvarios/AAPL2.csv'},
    {'ticker': 'QQQ',
     'path': work_path + '/archivosvarios/QQQ.csv'},
    {'ticker': 'QQQ',
     'path': work_path + '/archivosvarios/QQQ2.csv'},
    {'ticker': 'QQQ',
     'path': work_path + '/archivosvarios/dta_qqq.parquet'},
    {'ticker': 'QQQ',
     'path': work_path + '/archivosvarios/dta_qqq250220.parquet'},
    {'ticker': 'SPY',
     'path': work_path + '/archivosvarios/dta_spy.parquet'},
    {'ticker': 'SPY',
     'path': work_path + '/archivosvarios/dta_spy250220.parquet'},
    {'ticker': 'SPY',
     'path': work_path + '/archivosvarios/dta_spy_250306_10.parquet'},

]


class FakeStreamer:

    def __init__(self, generator, output_queue):

        self.event_queue = output_queue
        self.connect_csv_endpoint(generator)

    def connect_csv_endpoint(self,
                             generator: Generator):

        self.shutdown_event = threading.Event()
        self.thread = threading.Thread(
            target=self._setup_connect_csv_endpoint,
            args=(generator, ))
        self.thread.start()

    def close_csv_endpoint(self):
        if self.shutdown_event.is_set():
            logger.info('csv_endpoint was closed already')
        else:
            self.shutdown_event.set()

    def _setup_connect_csv_endpoint(self,
                                    generator: Generator):
        # import pyarrow.parquet as pq

        # sche = pq.read_schema(work_path + '/synthetic_server_path/us_equity.parquet')

        logger.debug('started streaming from csv endpoint')

        for beat in generator:
            self.event_queue.put({'data': beat,
                                  'table': 'us_equity'})
            time_stamp = beat['date'][0]
            logger.debug(f"data streamed from csv_api {time_stamp}")

            if self.shutdown_event.is_set():
                print('csv connection closed')
                logger.info('csv connection closed')
                return


class DBMaintainer:

    def __init__(self, event_queue, equity_table):
        """

        :param event_queue:
        :param equity_table: must be ParquetHandler
        """
        self.start_db_maintainer()
        self.event_queue = event_queue
        self.equity_table = equity_table

    def start_db_maintainer(self):
        self.shutdown_event = threading.Event()

        start_event = threading.Event()

        self.thread = threading.Thread(
            target=self._setup_db_maintainer,
            args=(start_event,))

        self.thread.start()

        while not start_event.is_set():
            continue

        logger.debug('DataMaintainer process started')

    def stop_db_maintainer(self):
        if self.shutdown_event.is_set():
            logger.info('db maintainer was stopped already')
        else:
            self.shutdown_event.set()

    def _setup_db_maintainer(self, start_event):

        self.rab_con = RabbitConnection()
        # self.rab_connections['handler'] = rab_con
        self.rab_con.channel.exchange_declare(**exchange_declarations['DataHandler'])

        start_event.set()

        while not self.shutdown_event.is_set():

            try:
                queue_item = self.event_queue.get(block=False)
            except queue.Empty:
                self.rab_con.connection.sleep(1)
                continue

            if queue_item['table'] == 'us_equity':
                self.maintain_us_equity(queue_item)
            else:
                print(f"table name: {queue_item['table']} not recognized")

        self.rab_con.connection.close()
        logger.debug('Data maintainer was shutdown')

    def maintain_us_equity(self, queue_item):

        data = (
            queue_item['data']
            .with_columns(timestamp_write=dt.datetime.now())
        )

        self.equity_table.update(new_row=data,
                                 unique_subset=['date', 'ticker'],
                                 sleep_fun=self.rab_con.connection.sleep,
                                 sleep_time=1)

        tbl_name = queue_item['table']
        import string
        timestamp = data['date'][0]
        mensaje = {'timestamp': timestamp.strftime('%Y%m%d%H%M%S'),
                   'table_name': tbl_name,
                   'tickers': data['ticker'].to_list(),
                   'event': 'new_data',
                   'large_shit': [string.printable] * 10000}  # delete this later

        self.rab_con.channel.basic_publish(
            exchange='exchange_data_handler',
            routing_key=f'data_csv.{tbl_name}',
            body=json.dumps(mensaje),
            properties=pika.BasicProperties(content_type='application/json'))

        logger.debug(f"updated tbl---{tbl_name}---sent: {timestamp.strftime('%c')}")
        self.event_queue.task_done()


class MockIbApi:
    """
    specificity triuhmps greedy generality
    """

    def __init__(self, stk_dict):

        self.stk_df = self._read_stk(stk_dict)

    @staticmethod
    def _read_stk(stk_dict):

        stacked_tables = []
        for entry in stk_dict:

            if entry['ticker'].lower() not in entry['path'].lower():
                print(f"ticker {entry['ticker']} is not in path:")
                print(f"{entry['path']}")
                raise Exception

            if 'parquet' in entry['path']:
                df_temp = (
                    pl.read_parquet(entry['path'])
                    .with_columns(
                        date=pl.col('date')
                        .dt.replace_time_zone(None)
                        .dt.cast_time_unit('ms'),
                        ticker=pl.lit(entry['ticker']))
                )

            elif 'csv' in entry['path']:
                df_temp = (
                    pl.read_csv(entry['path'])
                    .with_columns(
                        date=pl.col('date').str.to_datetime(
                            format='%Y-%m-%d %H:%M:%S%z',
                            time_zone='America/New_York',
                            time_unit='ms')
                        .dt.replace_time_zone(None),
                        ticker=pl.lit(entry['ticker']))
                )

            else:
                print('extension not recognized for file:')
                print(f"{entry['path']}")
                raise Exception

            stacked_tables.append(df_temp)

        df_stacked = pl.concat(items=stacked_tables,
                               how='vertical', rechunk=True)

        df_stacked = df_stacked.unique(subset=['date', 'ticker'])

        return df_stacked

    def fake_connection(self, filtro, pulse):
        # filtro = (pl.col('date') >= dt.datetime(2025,2,28)) & (pl.col('date') <= dt.datetime(2025, 3, 1))
        df_filtered = (
            self.stk_df
            .filter(filtro)
        )

        timestamps_filtered = df_filtered['date'].unique().sort(descending=False)

        def synthetic_stream():

            for t in timestamps_filtered:
                df_out = (
                    df_filtered
                    .filter(pl.col.date == t)
                )
                yield df_out

                time.sleep(pulse)

        return synthetic_stream()


mockib_instance = MockIbApi(dict_of_equity_files)


aea = pl.read_parquet(work_path + '/synthetic_server_path/us_equity.parquet')


class BasicDataHandler:

    def __init__(self,
                 table_info: dict):
        # <editor-fold desc="initializing the connection to the database">
        from os import path as os_path

        self.db_path = {}
        supported_tables = ['equity']

        for tbl, tbl_path in table_info.items():

            if tbl in supported_tables:
                if os_path.exists(tbl_path):
                    self.db_path[tbl] = tbl_path
                else:
                    logger.error(f'data base path not found for table: {tbl}')
                    raise Exception(f'data base path not found for table: {tbl}')
            else:
                logger.error(f"Table: '{tbl}' not supported")
                raise Exception(f"Table: '{tbl}' not supported")
        # </editor-fold>

        self.feeds = {}

        self.tbl_us_equity = ParquetHandler(
            work_path + '/synthetic_server_path/us_equity.parquet')
        self.queue_db_maintainer = queue.Queue()
        self.db_maintainer = DBMaintainer(
            event_queue=self.queue_db_maintainer,
            equity_table=self.tbl_us_equity)

        self.read_events = {
            'us_equity': self.tbl_us_equity.read_event, }
        self.rpc_read = DataHandlerRPC(self.read_events)

    def connect_ib_feed(self, generator, output_queue):

        self.feeds['ib'] = FakeStreamer(generator, output_queue)


class DataHandlerRPC:
    def __init__(self, dic_read_events):
        """
        RPC for updating reading events
        :param dic_read_event:
        """

        self.read_events = dic_read_events
        self.start_db_rpc_api()

    def start_db_rpc_api(self):

        connection_event = threading.Event()
        self.thread = threading.Thread(
            target=self._setup_db_rpc_api,
            args=(connection_event, ))

        self.thread.start()

        while not connection_event.is_set():
            pass
        logger.info('db_rpc_api started')

    def stop_db_rpc_api(self):

        rab_con_name = 'dh_rpc_api'
        if self.rab_con.connection.is_open:

            self.rab_con.stop_plus_close()

            if self.thread.is_alive():
                logger.warning(f'{rab_con_name} thread is still alive')
            else:
                logger.info(f'{rab_con_name} thread closed succesfully')
        else:
            logger.info(f'{rab_con_name} connection closed already')

    def _setup_db_rpc_api(self,
                          connection_event):

        name_queue = 'rpc_DataHandler'
        name_exchange = 'exchange_rpc_DataHandler'

        self.name_exchange = name_exchange
        self.rab_con = RabbitConnection()
        self.rab_con.channel.exchange_declare(
            exchange=name_exchange,
            exchange_type='direct',
            passive=False)

        self.rab_con.channel.queue_delete(queue=name_queue)
        self.rab_con.channel.queue_declare(
            queue=name_queue,
            passive=False,
            exclusive=True,
            auto_delete=False,
            arguments={'x-consumer-timeout': 180_000})

        self.rab_con.channel.queue_bind(
            queue=name_queue,
            exchange=name_exchange,
            routing_key=f"rt_{name_queue}")

        self.rab_con.channel.basic_qos(prefetch_count=1)
        self.rab_con.channel.basic_consume(
            queue=name_queue,
            on_message_callback=self.rpc_cllbck,
            exclusive=True)

        connection_event.set()
        self.rab_con.channel.start_consuming()
        logger.info('db_rpc_api stopped')

    def rpc_cllbck(self, ch, method, properties, body):
        if properties.content_type != 'application/json':
            raise Exception(f"Content type not json or specified")

        body = json.loads(body)
        table_name = body['table_name']
        if body['permission'] == 'acquire':
            self.read_events[table_name].set()
        elif body['permission'] == 'release':
            self.read_events[table_name].clear()

        self.rab_con.channel.basic_publish(
            exchange=self.name_exchange,
            # routing_key=body['routing_key'],
            routing_key=properties.reply_to,
            body=json.dumps({'response': f"event_set_to_{body['permission']}",
                             'table_name': table_name}),
            properties=pika.BasicProperties(
                content_type='application/json', )
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)







