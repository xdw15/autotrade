import pika

from libs.strategy import DumbStrat
from libs.config import *
import datetime as dt
import logging


logger = logging.getLogger('autotrade')
logger.setLevel(logging.DEBUG)
log_Formatter = logging.Formatter(
    fmt='{levelname:<10}---{name:<30}--{filename:<30}--{thread:<10}---{message:<40}--{asctime:12}',
    datefmt='%m/%d-%H:%M:%S',
    style='{',
    validate=True
)

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_Formatter)
logger.addHandler(log_stream_handler)


dumb_strat = DumbStrat(calibration_date=dt.date(2025, 1, 24),
                       ticker='QQQ',
                       signal_frequency=dt.timedelta(seconds=30))

logger.debug(f'this is{__name__}')
dumb_strat.connect_db_endpoint(
    exchange='exchange_data_handler',
    routing_key='data_csv.equity'
)

# dumb_strat.close_db_endpoint()

# dumb_strat.rab_connections['data_handler'].close_threadsafe()


# from libs.rabfile import *

# aea = RabbitConnection()
# aea.channel.queue_declare('xd',arguments={'x-consumer-timeout': 10000})
