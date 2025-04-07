import json
import polars as pl
import time
import threading
from typing import Iterable, Generator
import queue
from libs.rabfile import *
from libs.config import *
import datetime as dt
from libs.autoport_utils import *


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

    def fake_connection(self, filtro): # , pulse):
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

                # time.sleep(pulse)

        return synthetic_stream()


# mockib_instance = MockIbApi(dict_of_equity_files)
# aea = pl.read_parquet(work_path + '/synthetic_server_path/us_equity.parquet')


class FakeStreamer:

    def __init__(self, event_queue):

        self.event_queue = event_queue
        self.thread = threading.Thread()
        self.shutdown_event = threading.Event()
        # self.connect_generator(generator)

    def start(self, generator, pulse):

        self.shutdown_event.clear()
        self.thread = threading.Thread(
            target=self._setup,
            args=(generator, pulse, ))
        self.thread.start()

    def close(self):
        if self.thread.is_alive():
            if self.shutdown_event.is_set():
                logger.info('FakeStream was shutdown already')
            else:
                self.shutdown_event.set()
                while self.thread.is_alive():
                    continue
        else:
            logger.info('FakeStream is not alive')

    def _setup(self, generator, pulse):
        # import pyarrow.parquet as pq

        # sche = pq.read_schema(work_path + '/synthetic_server_path/us_equity.parquet')

        logger.debug('FakeStreamer started')
        t0 = time.perf_counter()
        for beat in generator:
            self.event_queue.put(
                {'data': beat,
                 'table': 'us_equity'})

            time_stamp = beat['date'][0]
            logger.info(f"data streamed from FakeStreamer {time_stamp}")

            if self.shutdown_event.is_set():
                print('csv connection closed')
                logger.info('csv connection closed')
                return
            time.sleep(pulse)

        print(f"FakeStream done, took {time.perf_counter() - t0} secs")


class DBMaintainerPublisher:

    def __init__(self):
        start_event = threading.Event()
        self.switch_rab_con_hb = threading.Event()

        self.thread = threading.Thread(
            target=self._setup_publisher,
            args=(start_event, ))
        # self._start_fill_publisher()
        self.thread.start()

        while not start_event.is_set():
            continue

        logger.info('DBMaintainerPublisher started')

    def _setup_publisher(self, _start_event, time_limit=1):

        self.switch_rab_con_hb.clear()
        self.rab_con = RabbitConnection()

        self.rab_con.channel.exchange_declare(
            **exchange_declarations['DataHandler'])

        _start_event.set()

        while not self.switch_rab_con_hb.is_set():
            self.rab_con.connection.process_data_events(
                time_limit=time_limit)

        logger.info(f"DBMaintainerPublisher stopped")

    def close(self):

        self.switch_rab_con_hb.set()
        while self.thread.is_alive():
            pass


class DBMaintainer:

    def __init__(self, data_handler, event_queue, equity_table):
        """
        :param event_queue:
        :param equity_table: must be ParquetHandler
        """
        self.data_handler = data_handler
        self.event_queue = event_queue
        self.equity_table = equity_table

        self.thread = threading.Thread()
        self.shutdown_event = threading.Event()

        # start_event = threading.Event()
        #
        # self.shutdown_event_publisher = threading.Event()
        # self.thread_publisher = threading.Thread(
        #     target=self.start_publisher,
        #     args=(start_event, )
        # )
        #
        # self.thread_publisher.start()
        #
        # while not start_event.is_set():
        #     continue

        self.start_db_maintainer()

        self.thread_auto_restart = threading.Thread(
            target=self.auto_restart,
            args=())
        # self.thread_auto_restart.start()

    def auto_restart(self):

        logger.info('DBMaintainer auto_restart started')
        while not self.shutdown_event.is_set():
            if not self.thread.is_alive():
                self.start_db_maintainer()
                logger.warning('DBMaintainer auto restarted')

        logger.info("DBMaintainer auto_restart finished")

    def start_db_maintainer(self):
        self.shutdown_event.clear()

        # start_event = threading.Event()

        self.thread = threading.Thread(
            target=self._setup_db_maintainer,
            args=())

        self.thread.start()

        logger.info('DB maintainer process started')

    def stop_db_maintainer(self):
        if self.shutdown_event.is_set():
            logger.info('db maintainer was stopped already')
        else:
            self.shutdown_event.set()

    # def start_publisher(self, start_event, time_limit=2):
    #
    #     self.shutdown_event_publisher.clear()
    #     self.rab_con = RabbitConnection()
    #     self.rab_con.channel.exchange_declare(
    #         **exchange_declarations['DataHandler'])
    #
    #     start_event.set()
    #     logger.info('DBMaintainer publisher started')
    #     while not self.shutdown_event_publisher.is_set():
    #         self.rab_con.connection.process_data_events(
    #             time_limit=time_limit)
    #
    #     logger.info('Data maintainer publisher was shutdown')

    def _setup_db_maintainer(self):

        while not self.shutdown_event.is_set():

            try:
                queue_item = self.event_queue.get(block=False)
            except queue.Empty:
                # time.sleep(1)
                continue

            if queue_item['table'] == 'us_equity':
                self.maintain_us_equity(queue_item)
            else:
                print(f"table name: {queue_item['table']} not recognized")

        # self.shutdown_event_publisher.set()
        logger.info('Data maintainer was shutdown')

    def maintain_us_equity(self, queue_item):

        data = (
            queue_item['data']
            .with_columns(timestamp_write=dt.datetime.now())
            .rename({'close': 'price'})
            .select(self.equity_table.sc.keys())

        )

        self.equity_table.update(new_row=data,
                                 unique_subset=['date', 'ticker'],
                                 sleep_fun=time.sleep,
                                 # sleep_fun=self.rab_con.connection.sleep,
                                 sleep_time=1)

        tbl_name = queue_item['table']
        import string
        timestamp = data['date'][0]
        mensaje = {'timestamp': timestamp.strftime('%Y%m%d%H%M%S'),
                   'table_name': tbl_name,
                   'tickers': data['ticker'].to_list(),
                   'event': 'new_data',
                   'large_shit': [string.printable] * 10000}  # delete this later

        rab_con = self.data_handler.dbmaint_publsiher.rab_con

        def publicar():
            rab_con.channel.basic_publish(
                exchange=exchange_declarations['DataHandler']['exchange'],
                routing_key=f'data_csv.{tbl_name}',
                body=json.dumps(mensaje),
                properties=pika.BasicProperties(content_type='application/json'))
            logger.info(f'published update for table {tbl_name}')

        rab_con.connection.add_callback_threadsafe(publicar)

        logger.info(f"updated tbl---{tbl_name}---sent: {timestamp.strftime('%c')}")
        self.event_queue.task_done()


class DataHandlerRPC:
    def __init__(self, dic_read_events):

        self.read_events = dic_read_events
        self.start_db_rpc_api()
        self.thread = threading.Thread()

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


class BasicDataHandler:

    def __init__(self,
                 table_info: dict):

        self.dbmaint_publsiher = DBMaintainerPublisher()

        # <editor-fold desc="checking if the tables do exist">
        from os import path as os_path

        self.db_path = {}
        supported_tables = ['us_equity']

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

        self.api_connections = {}

        self.tbl_us_equity = ParquetHandler(
            work_path + '/synthetic_server_path/us_equity.parquet')
        self.queue_db_maintainer = queue.Queue()

        self.read_events = {
            'us_equity': self.tbl_us_equity.read_event, }

        self.db_maintainer = DBMaintainer(
            data_handler=self,
            event_queue=self.queue_db_maintainer,
            equity_table=self.tbl_us_equity)

        self.fake_streamer = FakeStreamer(self.queue_db_maintainer)
        self.rpc_read = DataHandlerRPC(self.read_events)

    def connect_mock_api(self, generator, pulse):

        self.fake_streamer.start(generator, pulse)

    def stop_process(self):
        self.fake_streamer.close()
        self.db_maintainer.stop_db_maintainer()
        self.dbmaint_publsiher.close()
        self.rpc_read.stop_db_rpc_api()

        logger.info('Closed all DataHandler processes')




