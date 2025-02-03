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
    fmt='---{name:<30}--{filename:<30}--{levelname:<10}--{message:<40}--{asctime:12}',
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

    'Equity': [
        {
            'Ticker': 'QQQ',
            'path': work_path + "/archivosvarios/QQQ2.csv",
            'col_names': {'date': 'date', 'price': 'average'}
        },
        {
            'Ticker': 'AAPL',
            'path': work_path + "/archivosvarios/AAPL2.csv",
            'col_names': {'date': 'date', 'price': 'average'}
        },
    ]
}

csv_client = DataAPICSV(csvinfo)

csv_client_connection = csv_client.client_datafeed()


db_connections = {
    'Equity': work_path + '/synthetic_server_path/us_equity.parquet'
}
db_handler_app = DataHandlerPrimer(db_connections)

db_handler_app.setup_endpoint_csv(['Equity'],
                                  csv_client_connection(10)
                                  )

# db_handler_app.kill_keys['csv_endpoint'].set()

# for i in csv_client_connection(0):
#     print(i)
#     i['Equity']['date'][0].strftime('%Y%m%d%H%M%S')
