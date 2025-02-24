import json
import polars as pl
import pika
import logging
import datetime as dt
import time
from libs.config import *
from libs.rabfile import *
from libs.autoport_utils import *

logger = logging.getLogger('autotrade.' + __name__)


class AutoRiskManager:
    def __init__(self, auto_portfolio):
        self._auto_portfolio = auto_portfolio
        self.pass_through_orders = True
        logger.info('Risk Manager started')

        self.order_confirmations = {}

    def confirm_trade(self, order):
        if order['secType'] == 'STK':
            self.confirm_trade_stk(order)
        else:
            logger.warning(f"order_{order['order_itag']} not processed, secType not supported")

    def confirm_trade_stk(self, order):
        time.sleep(0.01)
        if not self.pass_through_orders:
            self.order_confirmations['order_itag'] = 0
            logger.info(f"order_{order['order_itag']} not confirmed")
            return

        # create the order

        order_body = {'symbol': order['symbol'],
                      'orderType': 'LMT',
                      'action': order['action'],
                      'totalQuantity': 1,  # for now
                      'lmtPrice': order['signalPrice'],
                      'secType': 'STK',
                      'origination_time_stamp': order['time_stamp'],
                      'confirmation_time_stamp': dt.datetime.now().strftime('%y%m%d%H%M%S'),
                      'order_itag': order['order_itag']}

        rab_conections = self._auto_portfolio.rab_connections

        if (not ('AutoExecution_client' in rab_conections)
                or rab_conections['AutoExecution_client'].is_closed):
            logger.warning(f"order_{order['order_itag']} can't be confirmed."
                           + f"AutoExecution client is not open or doesn't exist")

            self.order_confirmations['order_itag'] = -1
            return

        pika_basic_params = pika.BasicProperties(content_type='application/json',
                             reply_to=all_routing_keys['AutoExecution_client'],
                             correlation_id=order_body['order_itag'])

        publish_order_params = {"exchange": exchange_declarations['OrderExecution']['exchange'],
                                "routing_key": all_routing_keys['AutoExecution_server'],
                                "body": json.dumps(order_body),
                                "properties": pika_basic_params}

        rab_conections['AutoExecution_client'].connection.add_callback_threadsafe(
            lambda: rab_conections['AutoExecution_client'].channel.basic_publish(
                **publish_order_params
            )
        )

        self.order_confirmations['order_itag'] = 1

        new_row = {'order_id': order['order_itag'], 'status': 'dispatchedbyRM',
                   'note': '', 'timestamp': dt.datetime.now()}
        overrides = {'timestamp': pl.Datetime(time_unit='ms')}

        with self._auto_portfolio.lock_readwrite:
            update_blotter(new_row=new_row, overrides=overrides)

        logger.debug(f"order_{order['order_itag']} confirmed and dispatched for execution")
