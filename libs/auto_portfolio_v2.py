import threading
import queue
import typing
import numpy as np
import polars as pl
from abc import ABC, abstractmethod
import pika
import datetime as dt
import json
from collections import deque
from libs.risk_manager import *

#from charset_normalizer.md import getLogger
from libs.config import *
from libs.rabfile import RabbitConnection
from libs.autoport_utils import *
import logging

logger = logging.getLogger('autotrade.' + __name__)


class AutoPortDataHandlerEndpoint:

    def __init__(self, event_queue):
        self.event_queue = event_queue
        self.connect_db_endpoint()

    def connect_db_endpoint(self):
        connection_event = threading.Event()
        self.thread = threading.Thread(
            target=self._setup_db_endpoint,
            args=(connection_event, ))

        self.thread.start()

        while not connection_event.is_set():
            continue

        logger.info('DH_endpoint started')

    def close_db_endpoint(self):

        rab_con_name = 'DH_endpoint'
        if self.rab_con.connection.is_open:
            self.rab_con.stop_plus_close()
            if self.thread.is_alive():
                logger.warning(f'{rab_con_name} thread is still alive')
            else:
                logger.info(f'{rab_con_name} thread closed succesfully')
        else:
            logger.info(f'{rab_con_name} connection closed already')

    def _setup_db_endpoint(self,
                           connection_event: threading.Event):

        # exchange = exchange or 'exchange_data_handler'
        self.rab_con = RabbitConnection()

        self.rab_con.channel.exchange_declare(
            **exchange_declarations['DataHandler'])

        queue_declare = self.rab_con.channel.queue_declare(
            **queue_declarations['AutoPort_DH_endpoint'])

        queue_declare = queue_declare.method.queue

        deque((self.rab_con.channel.queue_bind(
            queue=queue_declare,
            exchange=exchange_declarations['DataHandler']['exchange'],
            routing_key=rt)
            for rt in all_routing_keys['AutoPort_DH_endpoint']),
            maxlen=0)

        self.rab_con.channel.basic_consume(
            queue=queue_declare,
            on_message_callback=self.db_cllbck_f,
            auto_ack=False)

        connection_event.set()
        self.rab_con.channel.start_consuming()

    def db_cllbck_f(self, ch, method, properties, body):
        if properties.content_type != 'application/json':
            logger.warning('content type not recognized')
            logger.warning('DH endpoint did not process msg')
            return

        body = json.loads(body)

        if body['table']['name'] not in autoport_tables:
            logger.info("the event's table name is not in the config")
            return

        self.event_queue.put(
            {'table_name': body['table']['name'],
             'timestamp': body['timestamp']}
        )
        ch.basic_ack(method.delivery_tag)

    def db_cllbck(self, ch, method, properties, body):
        # hay que arreglar esta wevada

        db_directory = {
            'equity': db_path + '/us_equity.parquet'
        }

        if properties.content_type != 'application/json':
            logger.warning('content type not json for message sent to db client')
            logger.warning('event not processed')
            ch.basic_nack(method.delivery_tag)
            return

        ch.basic_ack(method.delivery_tag)

        msg = json.loads(body)
        msg_security_type = msg['security']['type']
        msg_securities = body['security']['tickers']
        msg_time_stamp = dt.datetime.strptime(body['time_stamp'], '%Y%m%d%H%M%S')

        positions_last = (
            self.positions[msg_security_type]
            .filter(pl.col('date') == pl.col('date').max())
        )
        positions_last_time_stamp = positions_last['date'][0]

        if not ((body['event'] == 'new_data') and (msg_time_stamp > positions_last_time_stamp)):
            logger.warning(f'event new_data has a time stamp older than last position')
            logger.warning(f'event not processed')
            return

        payload_fields = {
            sec: True if sec in msg_securities else False
            for sec in positions_last['ticker']}

        if not all(payload_fields.values()):
            logger.warning('the following tickers are not present in the data sent')
            logger.warning(f'{[sec for sec, val in payload_fields.items() if val == False]}')
            logger.warning('event not processed')
            return

        df = (
            pl.scan_parquet(db_directory[msg_security_type])
            .filter(
                (pl.col('date') == msg_time_stamp)
                & (pl.col('ticker').is_in(msg_securities)))
            .collect()
        )

        self.event_queue['DH_endpoint'].put({'data': df,
                                             'time_stamp': msg_time_stamp})


class AutoExecRPCEndpoint:

    def __init__(self, autoport, event_queue):
        self.autoport = autoport
        self.event_queue = event_queue

        self._start_autoexecution_rpc_client()

    def _start_autoexecution_rpc_client(self):

        connection_event = threading.Event()
        self.thread = threading.Thread(
            target=self._setup_autoexecution_rpc_client,
            args=(connection_event, ))

        self.thread.start()

        while not connection_event.is_set():
            continue

        # starting the publisher that serves the rpc-publishing part
        self.flag_rab_con_hb = True

        self.thread_heartbeat = threading.Thread(
            target=self._heartbeat_rabcon,
            args=(self.rab_con,
                  self.flag_rab_con_hb,
                  'autoexecution_client'), )
        self.thread_heartbeat.start()

        logger.info('autoexecution_rpc_client started')

    def stop_endpoint(self):

        self.flag_rab_con_hb = False

        rab_con_name = 'autoexec rpc endpoint'
        if self.rab_con.connection.is_open:

            self.rab_con.stop_plus_close()

            if self.thread.is_alive():
                logger.warning(f'{rab_con_name} thread is still alive')
            else:
                logger.info(f'{rab_con_name} thread closed succesfully')
        else:
            logger.info(f'{rab_con_name} connection closed already')

    def _setup_autoexecution_rpc_client(self, _connection_event):

        self.rab_con = RabbitConnection()
        self.rab_con.channel.exchange_declare(
            **exchange_declarations['OrderExecution'])

        queue_declare = self.rab_con.channel.queue_declare(
            queue='',
            passive=False,
            auto_delete=True)

        queue_declare = queue_declare.method.queue
        rt_key = all_routing_keys['AutoExecution_client']
        self.rab_con.channel.queue_bind(
            queue=queue_declare,
            exchange=exchange_declarations['OrderExecution']['exchange'],
            routing_key=rt_key)

        _connection_event.set()
        self.rab_con.channel.basic_consume(
            queue=queue_declare,
            on_message_callback=self._callback_autoexecution,
            auto_ack=False)

    def _callback_autoexecution(self, ch, method, properties, body):
        if properties.content_type != 'application/json':
            ch.basic_nack(method.delivery_tag)
            logger.info('AutoExecution callback msg not processed')
            return

        body = json.loads(body)

        # new row for blotter log
        new_row = {'order_itag': body['order_itag'],
                   'status': 'fill_confirmed',
                   'note': '',
                   'timestamp': dt.datetime.now()}
        overrides = {'timestamp': pl.Datetime(time_unit='ms')}

        self.autoport.blot_log.update(
            new_row=new_row,
            overrides=overrides)

        self.event_queue.put(body['fill_timestamps'])

    def order_confirmation(self, body):

        self.autoport.order_counter += 1
        body['order_itag'] = (body['time_stamp'][2:]
                              + f'{self.autoport.order_counter:0>8}')

        # update blotter
        blot_newrow = {'timestamp': dt.datetime.now(),
                     'order_itag': body['order_itag'],
                     'secId': body['symbol'],
                     'secType': body['secType'],
                     'side': body['action'],
                     'tradeQty': body['totalQuantity'],
                     'orderType': body['orderType'],
                     'orderParams': str(body['lmtPrice']),
                     'placerId': body['placerId'],
                     # 'portAlloc': body['portAlloc'],
                     # 'typeAlloc': body['typeAlloc'],
        }

        overrides = {'timestamp': pl.Datetime(time_unit='ms')}

        self.autoport.blot.update(
            new_row=blot_newrow,
            overrides=overrides)

        # update blotter alloc

        new_row = []
        for port_key in body['portAlloc']:
            dic_unnested = {'timestamp': blot_newrow['timestamp'],
                            'order_itag': blot_newrow['order_itag'],
                            'port': port_key,
                            'Alloc': body['portAlloc'][port_key],
                            'typeAlloc': blot_newrow['typeAlloc']}
            new_row.append(dic_unnested)

        self.autoport.blot_alloc.update(
            new_row=new_row,
            overrides=overrides)

        # update blotter log

        new_row = {'order_itag': body['order_itag'],
                   'status': 'receivedByAutoPort',
                   'note': '',
                   'timestamp': dt.datetime.now()}

        self.autoport.blot_alloc.update(
            new_row=new_row,
            overrides=overrides)

        # order dispatched to risk manager

        logger.debug(f"order_{body['order_itag']} passed to risk_manager for confirmation")
        rm_output = self.autoport.risk_manager.confirm_trade(body)

        if rm_output['result'] is False:

            # update blotter log
            new_row = {'order_id': body['order_itag'],
                       'status': 'deniedByRM',
                       'note': '',
                       'timestamp': dt.datetime.now()}

            self.autoport.blot_log.update(
                new_row=new_row,
                overrides=overrides)

            return

        rab_con = self.rab_con

        if rab_con.connection.is_closed():
            logger.warning(f"order_{body['order_itag']} can't be confirmed."
                           + f"AutoExecution client is not open or doesn't exist")

            # update blotter log
            new_row = {'order_id': body['order_itag'],
                       'status': 'orderNotSent',
                       'note': 'AutoExec client is closed',
                       'timestamp': dt.datetime.now()}

            self.autoport.blot_log.update(
                new_row=new_row,
                overrides=overrides)

            return

        pika_basic_params = pika.BasicProperties(
            content_type='application/json',
            reply_to=all_routing_keys['AutoExecution_client'],
            correlation_id=body['order_itag'])

        publish_order_params = {
            "exchange": exchange_declarations['OrderExecution']['exchange'],
            "routing_key": all_routing_keys['AutoExecution_server'],
            "body": json.dumps(rm_output['order_body']),
            "properties": pika_basic_params}

        rab_con.connection.add_callback_threadsafe(
            lambda: rab_con.channel.basic_publish(
                **publish_order_params)
        )

        # update blotter log
        new_row = {'order_id': body['order_itag'],
                   'status': 'dispatchedToAutoExec',
                   'note': '', 'timestamp': dt.datetime.now()}

        self.autoport.blot_log.update(
            new_row=new_row,
            overrides=overrides)

        logger.debug(f"order_{body['order_itag']} confirmed and dispatched for execution")

    @staticmethod
    def _heartbeat_rabcon(con, flag, name, time_limit=1):

        logger.debug(f"{name} heartbeat started")
        while flag:
            con.connection.process_data_events(time_limit=time_limit)
        logger.debug(f"{name} heartbeat finished")


class OrderReceiver:

    def __init__(self, autoport):
        self.autoport = autoport
        self._start_order_receiver_endpoint()

    def _start_order_receiver_endpoint(self):

        connection_event = threading.Event()
        self.thread = threading.Thread(
            target=self._setup_order_receiver_endpoint,
            args=(connection_event, ))

        self.thread.start()

        while not connection_event.is_set():
            pass
        logger.info('OrderReceiver endpoint started')

    def _setup_order_receiver_endpoint(self,
                                       connection_event):

        self.rab_con = RabbitConnection()
        self.rab_con.channel.exchange_declare(
            **exchange_declarations["OrderReceiver"])
        queue_declare = self.rab_con.channel.queue_declare(
            **queue_declarations['OrderReceiver'])

        queue_declare = queue_declare.method.queue

        deque(
            (self.rab_con.channel.queue_bind(
                queue=queue_declare,
                exchange=exchange_declarations['OrderReceiver'],
                routing_key=routing_key)
                for routing_key
                in all_routing_keys['AutoPort_OrderReceiver'].values()),
            maxlen=0)

        self.rab_con.channel.basic_consume(
            queue=queue_declare,
            on_message_callback=self._order_receiver_cllbck,
            exclusive=True,
            auto_ack=False)

        connection_event.set()

        self.rab_con.channel.start_consuming()

    def _order_receiver_cllbck(self, ch, method, properties, body):

        if properties.content_type != 'application/json':
            logger.warning("an order sent to this application was not processed")
            ch.basic_nack(method.delivery_tag)
            return

        # the order is parsed and passed to the risk_manager app for confirmation
        body = json.loads(body)
        body['placerId'] = properties.app_id

        self.autoport.rpc_endpoint.order_confirmation(body)

        ch.basic_ack(method.delivery_tag)


class AutoRiskManager:
    def __init__(self, auto_portfolio):
        self.autoport = auto_portfolio
        self.pass_through_orders = False
        logger.info('Risk Manager started')

        self.order_confirmations = {}

    def confirm_trade(self, order):
        if order['secType'] == 'STK':
            self.confirm_trade_stk(order)
        else:
            logger.warning(f"order_{order['order_itag']} not processed, secType not supported")

    def confirm_trade_stk(self, order):
        output = {'result': True}

        time.sleep(0.01)
        if not self.pass_through_orders:
            output['result'] = False
            self.order_confirmations['order_itag'] = False
            logger.info(f"order_{order['order_itag']} not confirmed")
            return output

        # create the order

        order_body = {'symbol': order['symbol'],
                      'orderType': order['orderType'],
                      'action': order['action'],
                      'totalQuantity': order['totalQuantity'],  # for now
                      'lmtPrice': order['lmtPrice'],
                      'secType': order['secType'],
                      'origination_time_stamp': order['time_stamp'],
                      'confirmation_time_stamp': dt.datetime.now().strftime('%y%m%d%H%M%S'),
                      'order_itag': order['order_itag']}

        output['order_body'] = order_body

        return output

class AutoUpdatePort:

    def __init__(self, autoport, freq):

        self.autoport = autoport
        self.switch_event = threading.Event()
        self.freq = freq

    def start(self):

        self.switch_event.clear()
        self.thread = threading.Thread(
            target=self._setup_autoupdate,
            args=()
        )

        self.thread.start()

        print(f"AutoUpdate started")
    def _setup_autoupdate(self):

        while not self.switch_event.is_set():
            self.autoport.update_portfolio()
            time.sleep(self.freq)
    def stop(self):

        self.switch_event.set()

        while self.thread.is_alive():
            continue

        print(f"AutoUpdate stopped")

class ToyPortfolio:
    """
    a portfolio implementation that can deal with equities at first
    """

    def __init__(self,
                 start_timestamp,
                 start_auto_update = True,
                 freq_auto_update = 10,
                 price_ccy = 'USD'):
        """
        the class is a system that manages the updating
        of the holdings (securities and cash) and calculations (pnl, mtm, etc) tables
        in the context of subapplications generating live events.
        the existing subapps are
        - db client: receives data events to update the valuations
        - autoexec client: receives fill events to update positions. and sends orders received
        - order client: receives orders from other applications to which the subaplication is subscribed

        the class portfolio should be able to invoke the same tool that would update the table
        in a non-event fashion. i.e., through manually calling a script.
        :param start_timestamp:
        :param start_auto_update:
        :param freq_auto_update:
        :param price_ccy:
        """

        self.price_ccy = price_ccy
        self.freq_auto_update = freq_auto_update
        #start_timestamp = dt.datetime.strptime(time_stamp, '%Y%m%d%H%M%S')

        self.orders = ParquetHandler(work_path + '/synthetic_server_path/auto_exec/order_record.parquet')
        self.fills = ParquetHandler(work_path + '/synthetic_server_path/auto_exec/fill_record.parquet')

        self.blot = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter.parquet')
        self.blot_log = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter_log.parquet')
        self.blot_alloc = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter_alloc.parquet')

        self.q_equity = ParquetHandler(work_path + '/synthetic_server_path/auto_port/holdings/mock_equity.parquet')
        self.q_cash = ParquetHandler(work_path + '/synthetic_server_path/auto_port/holdings/mock_cash.parquet')

        self.p_equity = ParquetHandler(work_path + '/synthetic_server_path/us_equity.parquet')

        self.pos_handler = PositionHandler(
            positions_handle={'equity': self.q_equity,
                              'cash': self.q_cash},
            prices_handle={'us_equity': self.p_equity},
            fill_handle=self.fills,
            blot_alloc_handle=self.blot_alloc)

        self.order_counter = (
            self.orders.get_lazy()
            .with_columns(order_tstmp=pl.col.order_itag.str.slice(0, 6),
                          order_n=pl.col.order_itag.str.slice(6).cast(pl.Int64))
            .filter(pl.col.order_tstmp == start_timestamp.strftime('%y%m%d'))
            # .filter(pl.col.order_tstmp == '250219')
            .select(pl.col.order_n.max().drop_nulls())
            .collect()
        )

        if len(self.order_counter) == 0:
            self.order_counter = 0
        else:
            self.order_counter = self.order_counter[0, 0]

        self.q_data_event = queue.Queue()
        self.q_fill_event = queue.Queue()

        self.dh_endpoint = AutoPortDataHandlerEndpoint(self.q_data_event)
        self.rpc_endpoint = AutoExecRPCEndpoint(self, self.q_fill_event)
        self.order_receiver = OrderReceiver(self)
        self.risk_manager = AutoRiskManager(self)
        self.auto_update = AutoUpdatePort(self)

        if start_auto_update:

            self.auto_update.start()
        # ----------

        # self.positions = self._init_composition(
        #     initial_holdings,
        #     time_stamp
        # )

    def update_portfolio(self):

        events_data = self.process_data_queue()
        events_fill = self.process_fill_queue()

        events = pl.concat([events_data, events_fill])

        if len(events) == 0:
            return

        self.pos_handler.update_tables(events)

        timestamp = dt.datetime.now()
        print(f"AutoPort updated - {timestamp.strftime('%y%m%d-%H:%M:%S')}")

    def process_fill_queue(self):
        empty_event = False
        event_list = self.empty_queue(self.q_fill_event)
        if len(event_list) == 0:
            return pl.Series([], dtype=pl.Datetime(time_unit='ms'))

        filled_tags = []
        fill_timestamps = []
        for event in event_list:
            filled_tags.append(event['order_itag'])
            fill_timestamps = fill_timestamps + event['fill_timestamps']

        fill_timestamps = list(set(fill_timestamps))

        fill_timestamps = [dt.datetime.strptime(i, '%Y%m%d%H%M%S')
                           for i in fill_timestamps]

        series = pl.Series(fill_timestamps, dtype=pl.Datetime(time_unit='ms'))

        return series
    def process_data_queue(self):

        event_list = self.empty_queue(self.q_data_event)

        pl.concat(items=[pl.Series([], dtype=pl.Datetime(time_unit='ms')),
                         pl.Series([dt.datetime.now()], dtype=pl.Datetime(time_unit='ms'))])

        pl.Series([dt.datetime.now()], dtype=pl.Datetime(time_unit='ms')).sort(descending=False)
        if len(event_list) == 0:
            return pl.Series([], dtype=pl.Datetime(time_unit='ms'))

        event_list = [{k: msg[k]
                       for k in ['timestamp',
                                 'table_name',
                                 'tickers']}
                      for msg in event_list
                      if (msg['table_name'] in autoport_tables)
                      and (isinstance(msg['tickers'], list))]

        dates = list(set([d['timestamp'] for d in event_list]))

        series = pl.Series(
            [dt.datetime.strptime(dte, '%y%m%d%H%M%S')
             for dte in dates], dtype=pl.Datetime(time_unit='ms')
        )

        return series

    @staticmethod
    def empty_queue(cola):
        empty_event = False
        event_list = []
        while not empty_event:
            try:
                event_list.append(cola.get(block=False))
            except queue.Empty:
                empty_event = True
        return event_list




