import threading
import time

import numpy as np
import pandas as pd
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


def dt_to_series(datetime: dt.datetime,
                 time_unit_str: str = 'ms'):
    return pl.Series([datetime]).dt.cast_time_unit(time_unit_str)


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
        """
        returns the most recent date from the table
        """
        return self.get()[col_name].max()

    def addrow(self, new_row, overrides=None):
        """
        :param new_row: can be a dictionary or list of dicts
        :param overrides: dictionary of overrides
        :return: returns table with the new row
        """
        if isinstance(new_row, dict):
            new_row = [new_row]

        df = self.get()
        df_new_row = pl.DataFrame(data=new_row,
                                  # schema=df.columns,
                                  schema_overrides=overrides)
        df = pl.concat(items=[df, df_new_row], how='vertical', rechunk=True)
        return df

    def update(self, new_row, overrides=None):
        """
        :param new_row: can be a dict or list of dicsts
        :param overrides: dict of overrides
        :return: updates (writes) the table with the new row
        """
        updated_df = self.addrow(new_row, overrides)
        updated_df.write_parquet(self.path)

    def get_start_ts(self, event_timestamp_, timestamp_col='timestamp') -> pl.Series:
        """
        querys the table for the ts inmediatedly preceeding the event ts
        :param event_timestamp_: timestamp or series timestamp in ms unit
        :param timestamp_col: string name for the col
        :return: start timestamp in series format
        """
        # event_timestamp_ = event_dates
        if isinstance(event_timestamp_, pl.Series) and isinstance(event_timestamp_.dtype, pl.Datetime):
            event_timestamp_ = event_timestamp_.min()

        return (pos_equity.get()
         .with_columns(secs=(pl.col(timestamp_col) - event_timestamp_).dt.total_seconds())
         .with_columns(pl.when(pl.col.secs >= 0).then(None).otherwise('secs').alias('secs'))
         .filter(pl.col.secs == pl.col.secs.max())[timestamp_col].unique())

    def get_ts_to_update(self,
                         event_timestamp: pl.Series | dt.datetime,
                         timestamp_col='timestamp'):
        """
        returns a series whose first ts won't be modified. subsequent ts are created/recalculated.
        @param event_timestamp: series or datetime of events to be updated/added
        @param timestamp_col: defaults to timestamp
        @return:
        """
        pl.Series([dt.datetime(2023,1,1)]).dt.cast_time_unit(time_unit='ms')
        if isinstance(event_timestamp, dt.datetime):
            event_timestamp = pl.Series([event_timestamp, ])
        if not isinstance(event_timestamp.dtype, pl.Datetime):
            raise Exception(f'Series passed is {event_timestamp.dtype}, not Datetime')

        start_timestamp = self.get_start_ts(event_timestamp, timestamp_col)
        ts_to_update = self.get().filter(pl.col(timestamp_col) >= start_timestamp)[timestamp_col].unique()
        ts_to_update.append(event_timestamp.dt.cast_time_unit(time_unit='ms'))
        # this is to remove a duplicate event ts due to the ts being one already present in the table
        ts_to_update = ts_to_update.unique()
        ts_to_update = ts_to_update.sort(descending=False)

        return ts_to_update


class FillHandler(ParquetHandler):

    def __init__(self, path, blotter_handler):
        super().__init__(path)
        self.blotter_alloc = blotter_handler

    def fills_processing(self, date_start, date_end):
        """
        :param date_start: open (exclusive)
        :param date_end:  closed (inclusive)
        :return: fills processed
        """

        (
            fills.get().filter(
            pl.col('fill_timestamp').is_between(
                lower_bound=dt.datetime(2025, 2, 19, 11, 37,32),
                upper_bound=dt.datetime(2025, 2, 20, 16),
                closed='right'))
            .with_columns(
                trade_cost_basis=pl.when(pl.col.side == 'BUY')
                .then(pl.col.avgPrice * pl.col.tradeQty + pl.col.commission)
                .otherwise(-pl.col.avgPrice * pl.col.tradeQty + pl.col.commission),
                amount_variation=pl.when(pl.col.side == 'BUY')
                .then(pl.col.tradeQty)
                .otherwise(-pl.col.tradeQty))
        )

        fills_filtered = (
            self.get()
            .filter((date_end >= pl.col.fill_timestamp)
                    & (pl.col.fill_timestamp > date_start))
            # .with_columns(pl.Series([]))
            .select('secId', 'secType', 'side', 'avgPrice', 'commission', 'tradeQty', 'order_itag')
            .with_columns(
                trade_cost_basis=pl.when(pl.col.side == 'BUY')
                .then(pl.col.avgPrice * pl.col.tradeQty + pl.col.commission)
                .otherwise(-pl.col.avgPrice * pl.col.tradeQty + pl.col.commission),
                amount_variation=pl.when(pl.col.side == 'BUY')
                .then(pl.col.tradeQty)
                .otherwise(-pl.col.tradeQty))
            .group_by(['secType', 'secId', 'order_itag'])
            .agg(pl.col.trade_cost_basis.sum(),
                 pl.col.amount_variation.sum())
            .join(other=(self.blotter_alloc.get()
                         .unique(subset=['timestamp', 'order_itag', 'port'],
                                 keep='last')
                         .drop('timestamp')),
                  on='order_itag',
                  how='left',
                  coalesce=True)
            .with_columns(
                trade_cost_basis=pl.when(pl.col.typeAlloc == 'amount')
                                 .then((pl.col.Alloc / pl.col.amount_variation).abs())
                                 .otherwise(pl.col.Alloc) * pl.col.trade_cost_basis,
                amount_variation=pl.when(pl.col.typeAlloc == 'amount')
                                 .then(pl.col.amount_variation.sign())
                                 .otherwise(pl.col.amount_variation) * pl.col.Alloc,
            )
            .select('secType', 'secId', 'trade_cost_basis', 'amount_variation', 'port')
        )

        return fills_filtered


class PositionHandler(ParquetHandler):

    def __init__(self, path, fill_handle, cash_handle):
        super().__init__(path)
        self.fills = fill_handle
        self.cash = cash_handle

    def update_tables(self, event_ts):
        """
        a ver prueba
        @param event_ts:
        @return:
        """

        # event_ts_series = pl.Series([dt.datetime(2025, 1, 22, 10,0,1,3),
        #                              dt.datetime(2025, 1, 22, 10,0,1,4)])
        #
        # event_ts_series = dt.datetime(2025, 1, 22, 10,0,1,4)
        # event_ts = event_ts_series
        input_qualification = ((isinstance(event_ts,
                                           pl.Series)
                                and isinstance(event_ts.dtype,
                                               pl.Datetime))
                               or isinstance(event_ts, dt.datetime))

        if not input_qualification:
            raise Exception('input type not conforming')

        if isinstance(event_ts, pl.Series) and len(event_ts) > 1:
            start_timestamp = self.get_start_ts(event_ts.min())
        else:
            start_timestamp = self.get_start_ts(event_ts)

        # start_timestamp = self.get_start_ts(event_ts)

        start_position = (
            self.get()
            .filter(pl.col('timestamp') == start_timestamp)
            # .filter(pl.col.port == '1')
        )

        start_position_cash = (
            self.cash.get()
            .filter(pl.col('timestamp') == start_timestamp)
        )

        ts_to_update = self.get_ts_to_update(event_ts)

        agg_position = []
        agg_position_cash = []
        for ts_index in range(1, len(ts_to_update)):

            fills_filtered = self.fills.fills_processing(
                date_start=ts_to_update[ts_index - 1],
                date_end=ts_to_update[ts_index])

            end_position = (
                start_position  # .filter(pl.col('port')=='xd')
                .join(other=(fills_filtered  # .filter(pl.col.port=='33333')
                             .rename({'secId': 'ticker'})),
                      on=['ticker', 'port'],
                      how='full',
                      coalesce=True)
                .with_columns(timestamp=ts_to_update[ts_index])
                .with_columns([pl.col(col).fill_null(0)
                               for col in ['amount',
                                           'avgPrice',
                                           'trade_cost_basis',
                                           'amount_variation']])
                # se hace el calculo aca
                .with_columns(
                    avgPrice=(pl.col.amount * pl.col.avgPrice
                              + pl.col.trade_cost_basis) / (pl.col.amount + pl.col.amount_variation),
                    amount=pl.col.amount + pl.col.amount_variation
                )
                .select(start_position.columns)
                # .drop('trade_cost_basis', 'amount_variation')
            )

            end_position_cash = (
                start_position_cash
                .join(other=(fills_filtered
                             .group_by('port')
                             .agg(pl.col('trade_cost_basis').sum())),
                      on='port',
                      how='left',
                      coalesce=True)
                .with_columns(
                    timestamp=ts_to_update[ts_index],
                    trade_cost_basis=pl.col.trade_cost_basis.fill_null(0))
                .with_columns(
                    amount=pl.col.amount - pl.col.trade_cost_basis,
                    cost_basis=pl.col.cost_basis - pl.col.trade_cost_basis)
                .drop('trade_cost_basis')
            )

            agg_position.append(end_position)
            start_position = end_position
            agg_position_cash.append(end_position_cash)
            start_position_cash = end_position_cash

        agg_position = (
            pl.concat(items=agg_position, how='vertical')
            .with_columns(pl.col('timestamp').dt.cast_time_unit('ms'))
        )
        agg_position_cash = (
            pl.concat(items=agg_position_cash, how='vertical')
            .with_columns(pl.col('timestamp').dt.cast_time_unit('ms'))
        )

        new_table = (
            pl.concat(items=[self.get().filter(pl.col('timestamp') <= start_timestamp),
                             agg_position],
                      how='vertical', rechunk=True)
        )

        new_table_cash = (
            pl.concat(items=[self.cash.get().filter(pl.col('timestamp') <= start_timestamp),
                             agg_position_cash],
                      how='vertical', rechunk=True)
        )

        new_table.write_parquet(self.path)
        new_table_cash.write_parquet(self.cash.path)



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

pos_cash = ParquetHandler(work_path + '/synthetic_server_path/auto_port/holdings/cash.parquet')
fills = ParquetHandler(work_path + '/synthetic_server_path/auto_exec/fill_record.parquet')


orders = ParquetHandler(work_path + '/synthetic_server_path/auto_exec/order_record.parquet')
blotter = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter.parquet')
blotter_log = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter_log.parquet')
blotter_alloc = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter_alloc.parquet')
fills = FillHandler(work_path + '/synthetic_server_path/auto_exec/fill_record.parquet', blotter_alloc)
pos_equity = PositionHandler(work_path + '/synthetic_server_path/auto_port/holdings/equity.parquet',
                             fill_handle=fills,
                             cash_handle=pos_cash)
blotter.get()
blotter_log.get()
blotter_alloc.get()
fills.get()
orders.get()

pos_cash.get()

pos_equity.get()



event_timestamp = dt.datetime(2025, 2, 22, 11,37,32)
event_timestamp = dt.datetime(2025, 1, 22, 11,37,32)
event_timestamp = dt.datetime(2025, 2, 23)


df = pl.read_parquet(work_path + '/archivosvarios/dta_spy.parquet')

dta_existing = pl.read_parquet(work_path + '/synthetic_server_path/us_equity.parquet')

dta_existing
pos_equity.get().tail(10)
fills.fills_processing(dt.datetime(2025,2,18,16), dt.datetime(2025,2,19,16))

mtm_start_date = dt.datetime(2025, 1, 24, 9, 30)
mtm_prueba_date = dt.datetime(2025, 1, 24, 15, 30)


event_dates = (
    dta_existing
    .filter(
        (pl.col('date') >= mtm_start_date)
        & (pl.col('date') <= mtm_prueba_date)
    )
    .unique(['date'])
    .sort(by='date', descending=False)
    ['date']
)

t0 = time.time()
pos_equity.update_tables(event_dates)
t1 = time.time() - t0


pos_equity.get()['timestamp'].unique()


pos_equity.get().with_columns(trade_cost_basis=pl.lit(0.0))

aea = (
    pos_equity.get()
    .with_columns(trade_cost_basis=pl.lit(0.0))
    .join(other=dta_existing.rename({'date': 'timestamp'}),
          on=['timestamp', 'ticker'],
          coalesce=True,
          how='left')
    .with_columns(
        pl.col('price').fill_null(pl.col('avgPrice').mean().over('ticker')))
)


pl.concat(items=[aea, pos_equity.get().insert_column(0, pl.lit(0.0).alias('price'))], how='vertical')

(
    pos_equity.get()
    .filter(
        (pl.col('timestamp').dt.second().cast(pl.Int64).mod(5)!=0)
        # & ((pl.col('timestamp')- pl.duration(seconds=10)).dt.hour().cast(pl.Int64) != 22)
    )
    ['timestamp'].unique()
)

aea = pos_cash.get()['timestamp'].unique() == pos_equity.get()['timestamp'].unique()

aea.sum()

with pl.Config(tbl_rows=40):
    print(pos_equity.get()['timestamp'].unique())

event_dates

(
    pos_equity.get()
    .filter(
        (pl.col('timestamp') >= mtm_start_date)
        & (pl.col('timestamp') <= mtm_prueba_date)
    )
)


(
    pos_equity.get()
    .filter(pl.col('timestamp')==first_date_dta)
    .select('timestamp', 'ticker')
    .unique()
)


