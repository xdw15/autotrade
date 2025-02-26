import threading

import numpy as np
import polars as pl
import datetime as dt

from libs.config import work_path


# # <editor-fold desc="table construction">
# from libs.auxiliary_funs import *
# query_sit = sit_qs('20250124')
#
# mock_port = (
#     query_sit
#     # .filter(pl.col('Portafolio') == 2)
#     .filter(pl.col('TipoRenta') == '2')
#     .filter((pl.col('Name').str.contains('QQQ'))
#             | pl.col('Name').str.contains('SPY'))
#     .select('Cantidad', 'Portafolio',
#             'Name', 'VPNOriginal')
#     .with_columns((pl.col('VPNOriginal')/pl.col('Cantidad'))
#                   .alias('cost_basis'),
#                   pl.lit('USD').alias('ccy'),
#                   pl.col('Name').replace({'QQQ INVESCO': 'QQQ'}),
#                   pl.lit(dt.datetime(2025, 1, 24)).alias('timestamp')
#                   )
#     .rename(
#         {'Cantidad': 'amount',
#          'Portafolio': 'port',
#          'Name': 'ticker'}
#     )
#     .select(['timestamp', 'port', 'amount', 'cost_basis', 'ccy', 'ticker'])
# )
#
# mock_port.write_parquet(work_path +
#                         '/synthetic_server_path/auto_port/positions.parquet')
# data = pl.read_parquet(work_path + '/synthetic_server_path/us_equity.parquet')
# # </editor-fold>


# # <editor-fold desc="selecting cols for blotter">
# ae_fill_record = pl.read_parquet(work_path +
#                              '/synthetic_server_path/auto_exec/fill_record.parquet')
#
#
# ap_positions = pl.read_parquet(work_path
#                                + '/synthetic_server_path/auto_port/positions.parquet')
#
# cols_fill_record = ['secId', 'secType', 'side', 'tradeQty', 'avgPrice', 'commission', 'order_itag', 'fill_timestamp']
#
#
# ap_blotter = ae_fill_record.select(cols_fill_record)
#
#
# dta = {'order_id': ['order1'], 'status': ['filled'],
#        'note': ['manual entry'], 'timestamp': [dt.datetime.now()]}
#
# df = pl.DataFrame(dta,
#                   schema_overrides={'timestamp': pl.Datetime(time_unit='ms')})
#
# df.write_parquet(work_path
#                  + '/synthetic_server_path/auto_port/blotter.parquet')
#
# df = pl.read_parquet(work_path
#                      + '/synthetic_server_path/auto_port/blotter.parquet')
# # </editor-fold>


class ParquetHandler:

    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        sc = pl.read_parquet_schema(path)
        del sc

    def get(self):
        df = pl.scan_parquet(self.path).collect()
        return df

    def get_lastdate(self, col_name='timestamp'):
        return self.get()[col_name].max()

    def addrow(self, new_row, overrides=None):
        if isinstance(new_row, dict):
            new_row = [new_row]

        df = self.get()
        df_new_row = pl.DataFrame(data=new_row,
                                  # schema=df.columns,
                                  schema_overrides=overrides)
        df = pl.concat(items=[df, df_new_row], how='vertical', rechunk=True)
        return df

    def update(self, new_row, overrides=None):
        updated_df = self.addrow(new_row, overrides)
        updated_df.write_parquet(self.path)


def update_blotter(new_row, overrides):
    path = work_path + '/synthetic_server_path/auto_port/blotter.parquet'
    df = get_blotter()
    updated_df = pl_addrow(df, new_row, overrides=overrides)
    updated_df.write_parquet(path)


def get_blotter():
    path = work_path + '/synthetic_server_path/auto_port/blotter.parquet'
    df = pl.read_parquet(path)
    return df


def pl_addrow(df, new_row, overrides=None):
    df_new_row = pl.DataFrame(data=[new_row],
                              schema_overrides=overrides)

    df = pl.concat(items=[df, df_new_row], how='vertical', rechunk=True)
    return df


p0 = {'equity': pl.read_parquet_schema(work_path + '/synthetic_server_path/auto_port/holdings/equity.parquet'),
      'cash': pl.read_parquet_schema(work_path + '/synthetic_server_path/auto_port/holdings/cash.parquet')}


pos_equity = ParquetHandler(work_path + '/synthetic_server_path/auto_port/holdings/equity.parquet')
blotter_log = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter_log.parquet')
fills = ParquetHandler(work_path + '/synthetic_server_path/auto_exec/fill_record.parquet')
orders = ParquetHandler(work_path + '/synthetic_server_path/auto_exec/order_record.parquet')

blotter_log.get()
fills.get()
orders.get()


fills.get().columns
orders.get()
fills.get()['order_itag']

date_event_timestamp = dt.datetime(2025, 1, 28, 10,0,0)


blotter.get().filter(pl.col('timestamp') > date_event_timestamp)

start_timestamp = (pos_equity.get()
 .with_columns(secs=(pl.col.timestamp - date_event_timestamp).dt.total_seconds())
 .with_columns(pl.when(pl.col.secs > 0).then(None).otherwise('secs').alias('secs'))
 .filter(pl.col.secs == pl.col.secs.max())['timestamp'].unique()
)

# adding one day fwd

latest_date = pos_equity.get_lastdate()
ports = ['1', '2', '3']
last_positions = (
    pos_equity.get()
    .filter(pl.col.port.is_in(ports)
            & (pl.col.timestamp == latest_date))
)

fills.get()['order_itag']

cols_fill_record = ['secId', 'secType', 'side', 'tradeQty', 'avgPrice', 'commission', 'order_itag', 'fill_timestamp']

fills.get()[cols_fill_record]

blotter_log


blotter_log.get().filter(
    (pl.col.timestamp >= date_event_timestamp)
    & (pl.col.status == 'filled')
)



