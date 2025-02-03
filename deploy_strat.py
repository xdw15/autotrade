from libs.strategy import DumbStrat
from libs.config import *
import datetime as dt

dumb_strat = DumbStrat(
    dt.date(2025,1,24),
    'QQQ'
)

dumb_strat.endpoint_data_handler(
    exchange='exchange_data_handler',
    routing_key='data_csv.Equity'
)

dumb_strat.kill_switch['data_handler'].connection.close()