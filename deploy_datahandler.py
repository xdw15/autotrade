import pika

from libs.config import *
from libs.data_handler_v2 import *
import logging

logger = logging.getLogger('autotrade')
logger.setLevel(logging.INFO)
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
log_Formatter = logging.Formatter(
    fmt='{levelname:<10}---{name:<30}--{filename:<30}--{thread:<10}---{message:<40}--{asctime:12}',
    datefmt='%m/%d-%H:%M:%S',
    style='{',
    validate=True
)
logger.addHandler(log_FileHandler)
log_FileHandler.setFormatter(log_Formatter)

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_Formatter)
logger.addHandler(log_stream_handler)

equity_files = [
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


mockib_instance = MockIbApi(equity_files)

table_dic = {'us_equity': work_path + '/synthetic_server_path/us_equity.parquet'}

table_inst = {key: ParquetHandler(val) for key, val in table_dic.items()}


db_instance = BasicDataHandler(table_info=table_dic)

generador = mockib_instance.fake_connection(
    pl.col('date').dt.date() == dt.date(2025, 2, 20))

db_instance.connect_mock_api(
    generator=generador,
    pulse=10)


table_inst['us_equity'].get()
# db_instance.stop_process()

# (
#     mockib_instance.stk_df
#     .filter(
#         pl.col('date').dt.date() == dt.date(2025, 2, 20)
#     )
# )


# ----------- scratch
#
# import ib_async as ib
#
# ib_con = ib.IB()
# ib_con.connect(port=4001, clientId=2)
#
# ib_con.positions()
#
# contract = ib.Contract(secType='STK', symbol='QQQ', exchange='SMART', currency='USD')
# contract = ib_con.qualifyContracts(contract)[0]
# ib_con.reqHistoricalData()
# live_bars = ib_con.reqRealTimeBars(contract=contract, barSize=5, whatToShow='TRADES', useRTH=True)
#
# def on_update(a, b):
#     print(ib.util.df(a))
#     print(b)
#
# live_bars.updateEvent += on_update
#
# ib_con.updateEvent()
# ib_con.run()