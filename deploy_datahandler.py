import pika

from libs.config import *
from libs.data_handler import *
import logging

logger = logging.getLogger('autotrade')
logger.setLevel(logging.DEBUG)
log_FileHandler = logging.FileHandler(
    filename= work_path
    + f'/{__name__}_prueba_data_handler.log',
    mode='w',
    encoding='UTF-8'
)
log_Formatter = logging.Formatter(
    fmt='{levelname:<10}---{name:<30}--{filename:<30}--{message:<40}--{asctime:12}',
    datefmt='%m/%d-%H:%M:%S',
    style='{',
    validate=True
)
logger.addHandler(log_FileHandler)
log_FileHandler.setFormatter(log_Formatter)

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_Formatter)
logger.addHandler(log_stream_handler)

csvinfo = {

    'equity': [
        {
            'ticker': 'DTCR',
            'path': work_path + "/archivosvarios/dta_dtcr250220.parquet",
            'col_names': {'date': 'date', 'price': 'average'}
        },
        # {
        #     'ticker': 'AAPL',
        #     'path': work_path + "/archivosvarios/AAPL2.csv",
        #     'col_names': {'date': 'date', 'price': 'average'}
        # },
    ]
}

aeap = pl.read_parquet(work_path + "/archivosvarios/dta_dtcr.parquet")

csv_client = DataAPICSV(csvinfo)

csv_client_connection = csv_client.client_datafeed(pulse=0.01)

db_connections = {
    'equity': work_path + '/synthetic_server_path/us_equity.parquet'
}
db_handler_app = DataHandlerPrimer(db_connections)
# db_handler_app.start_db_maintainer()

db_handler_app.connect_csv_endpoint(securities=['equity'],
                                    generator=csv_client_connection)


# db_handler_app.start_db_rpc_api()
# db_handler_app.stop_db_rpc_api()

#aea = pl.read_parquet(db_connections['equity'])


# ----------- scratch

import ib_async as ib

ib_con = ib.IB()
ib_con.connect(port=4001, clientId=2)

ib_con.positions()

contract = ib.Contract(secType='STK', symbol='QQQ', exchange='SMART', currency='USD')
contract = ib_con.qualifyContracts(contract)[0]
ib_con.reqHistoricalData()
live_bars = ib_con.reqRealTimeBars(contract=contract, barSize=5, whatToShow='TRADES', useRTH=True)

def on_update(a, b):
    print(ib.util.df(a))
    print(b)

live_bars.updateEvent += on_update

ib_con.updateEvent()
ib_con.run()