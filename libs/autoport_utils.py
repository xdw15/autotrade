import threading
import numpy as np
import polars as pl
import datetime as dt

from libs.config import work_path


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


p0 = {'equity': pl.read_parquet_schema(work_path + '/synthetic_server_path/auto_port/holdings/equity.parquet'),
      'cash': pl.read_parquet_schema(work_path + '/synthetic_server_path/auto_port/holdings/cash.parquet')}


pos_equity = ParquetHandler(work_path + '/synthetic_server_path/auto_port/holdings/equity.parquet')
blotter = ParquetHandler(work_path + '/synthetic_server_path/auto_port/blotter.parquet')
fills = ParquetHandler(work_path + '/synthetic_server_path/auto_exec/fill_record.parquet')

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

fills.get()

cols_fill_record = ['secId', 'secType', 'side', 'tradeQty', 'avgPrice', 'commission', 'order_itag', 'fill_timestamp']

blotter.get().filter(
    (pl.col.timestamp >= date_event_timestamp)
    & (pl.col.status == 'filled')
)



