import sys
import logging
from libs.data_handler import DataHandlerCSV

logger = logging.getLogger('autotrade')
logger.setLevel(logging.DEBUG)
log_FileHandler = logging.FileHandler(
    filename='Z:/Benchmarking/py_integra'
    + f'/PyCharmProjects/autotrade/{__name__}scratchbutno.log',
    mode='w',
    encoding='UTF-8'
)
log_Formatter = logging.Formatter(
    fmt='---{name:<30}--{filename:<30}--{levelname:<10}--{message:<40}--{asctime:12}',
    datefmt='%m/%d-%H:%M:%S',
    style='{',
    validate=True
)
logger.addHandler(log_FileHandler)
log_FileHandler.setFormatter(log_Formatter)


logger.info('log setup correcto')

csvinfo = {

    'Equity': [
        {
            'Ticker': 'QQQ',
            'path': r"Z:\Benchmarking\py_integra\PyCharmProjects\autotrade\archivosvarios\QQQ.csv",
            'col_names': {'date': 'date', 'price': 'average'}
        },
        {
            'Ticker': 'AAPL',
            'path': r"Z:\Benchmarking\py_integra\PyCharmProjects\autotrade\archivosvarios\AAPL.csv",
            'col_names': {'date': 'date', 'price': 'average'}
        },
    ]
}


aaa = DataHandlerCSV(csvinfo)
logger.debug('DataHandler successfully created')
aaa.client_datafeed(0)
logger.debug('All csv data has been streamed')



# import ib_async as iba
# ib = iba.IB()
# ib.connect(port=4001,clientId=7)
# ib.reqHistorical
#


