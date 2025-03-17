import pika

from libs.config import *
from libs.data_handler import *
import logging

looger = logging.getLogger('autotrade')
looger.setLevel(logging.DEBUG)
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
looger.addHandler(log_FileHandler)
log_FileHandler.setFormatter(log_Formatter)

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_Formatter)
looger.addHandler(log_stream_handler)

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

