import threading
import time
import polars as pl
import datetime as dt
from libs.config import work_path
import pyarrow.parquet as pa_parquet
import pyarrow as pa
import logging


logger = logging.getLogger('autotrade.' + __name__)


class ParquetHandler:

    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.read_event = threading.Event()
        self.sc = pl.read_parquet_schema(path)
        # del sc

    def _get_arrow(self):
        read_success = False
        read_trials = 0
        while (not read_success) and (read_trials < 3):
            try:
                df = pa_parquet.read_table(self.path)
                df = pl.from_arrow(df)
                read_success = True

            except (pa.lib.ArrowInvalid, OSError, Exception) as e:
                #print(e)
                logger.warning(e)
                read_trials += 1

        if read_success:
            return df
        else:
            raise Exception('could not read table')

    def get(self, filtro=None):

        with self.lock:

            if filtro is None:
                # df = pl.scan_parquet(self.path).collect()
                # df = pl.read_parquet(self.path)
                df = self._get_arrow()
            else:
                df = self._get_arrow().filter(filtro)
                # df = pl.scan_parquet(self.path).filter(filtro).collect()
            return df

    def get_lazy(self):
        with self.lock:
            # df = pl.scan_parquet(self.path)
            df = self._get_arrow().lazy()
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

        if isinstance(new_row, list):
            df_new_row = pl.DataFrame(data=new_row,
                                      # schema=df.columns,
                                      schema_overrides=overrides)
        elif isinstance(new_row, pl.DataFrame):
            df_new_row = new_row

        else:
            print('row could not be added, dtype of new row not admitted')
            return

        df = self.get()
        df = pl.concat(items=[df, df_new_row], how='vertical', rechunk=True)
        return df

    def update(self, new_row,
               overrides=None,
               unique_subset=None,
               sleep_fun=time.sleep,
               sleep_time=1):
        """
        :param new_row: can be a dict or list of dicsts
        :param overrides: dict of overrides
        :param sleep_fun: function to sleep, must be callable with one param
        :param sleep_time: self-explanatory
        :param unique_subset: allows for dropping duplicate rows. i.e., overwriting an entry.
        :return: updates (writes) the table with the new row
        """
        updated_df = self.addrow(new_row, overrides)

        if unique_subset is not None:
            updated_df = updated_df.unique(subset=unique_subset)

        self.write(updated_df,
                   sleep_fun=sleep_fun,
                   sleep_time=sleep_time)

    def write(self, new_df,
              sleep_fun=time.sleep,
              sleep_time=1):

        trials = 0
        while self.read_event.is_set() and (trials < 10):
            sleep_fun(sleep_time)
            trials += 1

        with self.lock:
            written_flag = False
            write_trials = 0
            while (not written_flag) and (write_trials < 3):
                try:
                    new_df.write_parquet(self.path)
                    written_flag = True
                except Exception as e:
                    print(e)
                    write_trials += 1

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

        return (self.get()
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


class PositionHandler:

    def __init__(self,
                 positions_handle,
                 prices_handle,
                 fill_handle,
                 blot_alloc_handle):

        # super().__init__(path)
        self.equity = positions_handle['equity']
        self.cash = positions_handle['cash']
        self.p_equity = prices_handle['us_equity']
        self.fills = fill_handle
        self.blot_alloc = blot_alloc_handle
        # self.cash = cash_handle

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

        # start timestamp is obtained from cash because cash table can never be empty
        if isinstance(event_ts, pl.Series) and len(event_ts) > 1:
            start_timestamp = self.cash.get_start_ts(event_ts.min())
        else:
            start_timestamp = self.cash.get_start_ts(event_ts)

        # start_timestamp = self.get_start_ts(event_ts)

        start_position = (
            self.equity.get()
            .filter(pl.col('timestamp') == start_timestamp)
            # .filter(pl.col.port == '1')
        )

        start_position_cash = (
            self.cash.get()
            .filter(pl.col('timestamp') == start_timestamp)
        )

        ts_to_update = self.cash.get_ts_to_update(event_ts)

        price_request_success = False
        while not price_request_success:
            try:
                p_equity_temp = (
                    self.p_equity.get()
                    # .get((pl.col('date') >= ts_to_update[0])
                    #         & (pl.col('date') <= ts_to_update[-1]))
                    .filter((pl.col('date') >= ts_to_update[0])
                            & (pl.col('date') <= ts_to_update[-1]))
                    .rename({'date': 'timestamp'})
                    .with_columns(timestamp=pl.col('timestamp').dt.cast_time_unit('us'))
                )
                price_request_success = True
            except (OSError, pl.exceptions.ComputeError, ) as e:
                print('catched an error')
                print(e)
                continue

        agg_position = []
        agg_position_cash = []
        for ts_index in range(1, len(ts_to_update)):

            fills_filtered = self.fills_processing(
                date_start=ts_to_update[ts_index - 1],
                date_end=ts_to_update[ts_index])

            end_position = (
                start_position# .filter(pl.col('port')=='xd')
                # drop zero positions
                .filter(pl.col.amount != 0.0)
                .rename({'mtm': 'mtm_prev',
                         'amount': 'amount_prev'})
                .drop('trade_cost_basis', 'price', 'price_source', 'pnl', 'ret')
                .join(other=(fills_filtered  # .filter(pl.col.port=='33333')
                             .rename({'secId': 'ticker'})),
                      on=['ticker', 'port'],
                      how='full',
                      coalesce=True)
                .with_columns(timestamp=ts_to_update[ts_index])
                             # mtm=None)
                .with_columns([pl.col(col).fill_null(0.0)
                               for col in ['amount_prev',
                                           'avgPrice',
                                           'trade_cost_basis',
                                           'amount_variation',
                                           'mtm_prev']])
                # se hace el calculo aca
                .with_columns(
                    avgPrice=(pl.col.amount_prev * pl.col.avgPrice
                              + pl.col.trade_cost_basis) / (pl.col.amount_prev + pl.col.amount_variation),
                    amount=pl.col.amount_prev + pl.col.amount_variation
                )
                .with_columns(
                    avgPrice=pl.when(pl.col('avgPrice').is_infinite())
                    .then(pl.lit(0)).otherwise(pl.col('avgPrice'))
                )
                # la parte de precios y pnl aca
                .join(other=p_equity_temp,
                      on=['timestamp', 'ticker'],
                      how='left',
                      coalesce=True)
                .with_columns(
                    price=pl.when(pl.col('price').is_null())
                    .then(pl.when(~pl.col('trade_avgPrice').is_null())
                          .then(pl.col('trade_avgPrice'))
                          .otherwise(pl.col('mtm_prev')
                                     / pl.col('amount_prev')))
                    .otherwise(pl.col('price')),
                    price_source=pl.when(pl.col('price').is_null())
                    .then(pl.when(~pl.col('trade_avgPrice').is_null())
                          .then(pl.lit('trade'))
                          .otherwise(pl.lit('last_mtm')))
                    .otherwise(pl.lit('vector')))
                .with_columns(
                    mtm=pl.col('price') * pl.col('amount'),
                    pnl=pl.col('price') * pl.col('amount') - pl.col('trade_cost_basis') - pl.col('mtm_prev'))
                .with_columns(ret=pl.when(pl.col('amount_prev') == 0.0)
                              .then(pl.col('mtm')/pl.col('trade_cost_basis')-1.0)
                              .otherwise(pl.col.pnl/pl.col.mtm_prev))
                .select(start_position.columns)
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
            pl.concat(items=[(self.equity.get()
                              .filter(pl.col('timestamp') <= start_timestamp)),
                             agg_position],
                      how='vertical', rechunk=True)
        )

        new_table_cash = (
            pl.concat(items=[(self.cash.get()
                              .filter(pl.col('timestamp') <= start_timestamp)),
                             agg_position_cash],
                      how='vertical', rechunk=True)
        )

        new_table.write_parquet(self.equity.path)
        new_table_cash.write_parquet(self.cash.path)

    def fills_processing(self, date_start, date_end):
        """
        :param date_start: open (exclusive)
        :param date_end:  closed (inclusive)
        :return: fills processed
        """

        fills_filtered = (
            self.fills.get()
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
            .join(other=(self.blot_alloc.get()
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
                .otherwise(pl.col.amount_variation) * pl.col.Alloc,)
            .with_columns(
                trade_avgPrice=pl.col('trade_cost_basis') / pl.col('amount_variation'))
            .select('secType', 'secId', 'trade_cost_basis', 'amount_variation', 'port', 'trade_avgPrice')
        )

        return fills_filtered


# orders = ParquetHandler(work_path + '/synthetic_server_path/auto_exec/order_record.parquet')
# fills = ParquetHandler(work_path + '/synthetic_server_path/auto_exec/fill_record.parquet')
#
# blot = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter.parquet')
# blot_log = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter_log.parquet')
# blot_alloc = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter_alloc.parquet')
#
# q_equity = ParquetHandler(work_path + '/synthetic_server_path/auto_port/holdings/mock_equity.parquet')
# q_cash = ParquetHandler(work_path + '/synthetic_server_path/auto_port/holdings/mock_cash.parquet')
#
# p_equity = ParquetHandler(work_path + '/synthetic_server_path/us_equity.parquet')
#
# pos = PositionHandler(
#     positions_handle={'equity': q_equity,
#                       'cash': q_cash},
#     prices_handle={'us_equity': p_equity},
#     fill_handle=fills,
#     blot_alloc_handle=blot_alloc)
