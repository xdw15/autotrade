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


def update_blotter(new_row, overrides):
    path = work_path + '/synthetic_server_path/auto_port/blotter.parquet'
    df = pl.read_parquet(path)
    updated_df = pl_addrow(df, new_row, overrides=overrides)
    updated_df.write_parquet(path)


def pl_addrow(df, new_row, overrides=None):
    df_new_row = pl.DataFrame(data=[new_row],
                              schema_overrides=overrides)

    df = pl.concat(items=[df, df_new_row], how='vertical', rechunk=True)
    return df
