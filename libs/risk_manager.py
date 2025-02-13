from libs.auto_portfolio import ToyPortfolio
from libs.rabfile import *
import logging
import datetime as dt

logger = logging.getLogger('autotrade.' + __name__)

class AutoRiskManager:
    def __init__(self, auto_portfolio):
        self._auto_portfolio = auto_portfolio
        self.pass_through_orders = True
        logger.info('Risk Manager started')

    def confirm_trade(self, order):
        order_confirmation_flag = True
        if not self.pass_through_orders:
            order_confirmation_flag = False
            logger.info(f"order_{order['order_tag']} not confirmed")
            return

        # create the order


        order_body = {'symbol': order['symbol'],
                      'orderType': 'LMT',
                      'action': order['action'],
                      'totalQuantity': 1,
                      'lmtPrice': order['signalPrice'],
                      'origination_time_stamp': order['time_stamp'],
                      'confirmation_time_stamp': dt.datetime.now().strftime('%y%m%d%H%M%S')}

        logger.debug(f"order_{order['order_tag']} confirmed and sent for execution")
