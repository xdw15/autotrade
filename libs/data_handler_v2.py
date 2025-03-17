import json
import polars as pl
import time
import threading
from typing import Iterable, Generator
import queue
from libs.rabfile import *
from libs.config import *
import datetime as dt

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
                    self.db_connection[tbl] = tbl_path
                else:
                    logger.error(f'data base path not found for table: {tbl}')
                    raise Exception(f'data base path not found for table: {tbl}')
            else:
                logger.error(f"Table: '{tbl}' not supported")
                raise Exception(f"Table: '{tbl}' not supported")
        # </editor-fold>

        self.feeds = {}

        # <editor-fold desc="ways to shut down stuff">
        self.shutdown_event = {}

        # </editor-fold>

        # <editor-fold desc="assigning other attributes">
        self.queue_db_handler = queue.Queue()
        self.rab_connections = {}
        self.read_permissions = {security: threading.Event()
                                 for security in supported_securities}
        self.thread_tracker = {}
        # </editor-fold>

        self.start_db_maintainer()
        self.start_db_rpc_api()

    def connect_ib_feed(self, generator, output_queue):

        self.feeds['ib'] = FakeStreamer(generator, output_queue)


class FakeStreamer:

    def __init__(self, generator, output_queue):

        self.master_queue = output_queue
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
            self.master_queue.put(
                {'data': beat, })
            time_stamp = beat['date'][0]
            logger.debug(f"data streamed from csv_api {time_stamp}")

            if self.shutdown_event.is_set():
                print('csv connection closed')
                logger.info('csv connection closed')
                return



class DBMaintainer:

    def __init__(self):
        self.start_db_maintainer()


    def start_db_maintainer(self):
        self.shutdown_event = threading.Event()

        self.shutdown_event = threading.Event()

        self.thread = threading.Thread(
            target=self._setup_db_maintainer,
            args=())
        self.thread.start()

    def stop_db_maintainer(self):
        if self.shutdown_event.is_set():
            logger.info('db maintainer was stopped already')
        else:
            self.shutdown_event.set()

    def _setup_db_maintainer(self):

        from libs.rabfile import RabbitConnection

        rab_con = RabbitConnection()
#        self.rab_connections['handler'] = rab_con
        rab_con.channel.exchange_declare(
            exchange='exchange_data_handler',
            exchange_type='topic',
            passive=False,
            auto_delete=False
        )

        logger.debug('DataHandler process started')

        while not self.shutdown_event['db_maintainer'].is_set():

            try:
                queue_item = self.queue_db_handler.get(block=False)
            except queue.Empty:
                rab_con.connection.sleep(1)
                continue

            security = queue_item['info']
            data = (
                queue_item['data']
                .with_columns(timestamp_write=dt.datetime.now())
            )

            temp_connection = pl.read_parquet(
                self.db_connection[security]
            )

            temp_connection = pl.concat(
                items=[temp_connection, data],
                how='vertical'
            )

            while self.read_permissions[security].is_set():
                rab_con.connection.sleep(1)

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

    def start_db_rpc_api(self):

        connection_event = threading.Event()
        t = threading.Thread(target=self._setup_db_rpc_api,
                             args=(connection_event,))

        self.thread_tracker['rpc_api'] = t
        t.start()

        with threading.Lock():
            while not connection_event.is_set():
                pass

        logger.info('db_rpc_api started')

    def stop_db_rpc_api(self):
        if self.rab_connections['rpc_api'].connection.is_open:
            (
                self.rab_connections['rpc_api']
                .connection.add_callback_threadsafe(
                    self.rab_connections['rpc_api']
                    .channel.stop_consuming
                )
            )

            (
                self.rab_connections['rpc_api']
                .connection.add_callback_threadsafe(
                    self.rab_connections['rpc_api']
                    .connection.close
                )
            )

            if self.thread_tracker['rpc_api'].is_alive():
                tag = 'open'
            else:
                tag = 'closed'
            logger.info(f'rpc_api closed, thread is {tag}')
        else:
            logger.info('rpc_api connection closed already')

    def _setup_db_rpc_api(self,
                          connection_event):

        name_exchange = 'exchange_data_handler_rpc'
        name_queue = 'rpc_data_handler'
        rab_con = RabbitConnection()
        self.rab_connections['rpc_api'] = rab_con
        rab_con.channel.exchange_declare(exchange=name_exchange,
                                         exchange_type='direct',
                                         passive=False)

        rab_con.channel.queue_delete(queue=name_queue)
        rab_con.channel.queue_declare(queue=name_queue,
                                      passive=False,
                                      exclusive=True,
                                      auto_delete=False,
                                      arguments={'x-consumer-timeout': 180_000})

        rab_con.channel.queue_bind(queue=name_queue,
                                   exchange=name_exchange,
                                   routing_key=f"rt_{name_queue}")

        def rpc_cllbck(ch, method, properties, body):
            if properties.content_type != 'application/json':
                raise Exception(f"Content type not json or specified")

            body = json.loads(body)
            if body['permission'] == 'acquire':
                self.read_permissions[body['security']].set()
            elif body['permission'] == 'release':
                self.read_permissions[body['security']].clear()

            rab_con.channel.basic_publish(
                exchange=name_exchange,
                routing_key=body['routing_key'],
                body=json.dumps({'response': 'granted'}),
                properties=pika.BasicProperties(
                    content_type='application/json',
                )
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        rab_con.channel.basic_qos(prefetch_count=1)
        rab_con.channel.basic_consume(queue=name_queue,
                                      on_message_callback=rpc_cllbck,
                                      exclusive=True)

        connection_event.set()
        rab_con.channel.start_consuming()
        logger.info('db_rpc_api stopped')









