import sys
from libs.auto_portfolio_v2 import *
import polars as pl
from libs.autoport_utils import *
# from libs.auxiliary_funs import sit_qs
import datetime as dt

logger = logging.getLogger('autotrade')
logger.setLevel(logging.INFO)
log_Formatter = logging.Formatter(
    fmt='{levelname:<10}---{name:<30}--{filename:<30}--{thread:<10}---{message:<40}--{asctime:12}',
    datefmt='%m/%d-%H:%M:%S',
    style='{',
    validate=True
)

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_Formatter)
logger.addHandler(log_stream_handler)


fecha_start = dt.datetime(2025, 2, 20)

tp = ToyPortfolio(
    start_timestamp=fecha_start,
    start_auto_update=True,
    freq_auto_update=10)



# import pyarrow.parquet as pa
#
# ja = pa.read_table('Z:/Benchmarking/py_integra/PyCharmProjects/autotrade/synthetic_server_path/us_equity.parquet')
#
#
# pl.from_arrow(ja).lazy()
#time.sleep(5)

#tp.stop_autoport()

# tp.update_portfolio()
#
# tp.rpc_endpoint.thread.is_alive()
with pl.Config(tbl_cols=15):
    print(tp.fills.get())
    print(tp.q_equity.get())




fecha_inicio = dt.datetime(2025, 2, 20, 10)
fecha_fin = fecha_inicio
fecha_fin = dt.datetime(2025, 2, 21, 11)


(
    tp.q_equity.get()
    .filter(
        (pl.col.timestamp >= fecha_inicio)
        & (pl.col.timestamp <= fecha_fin))
    .pivot(on=['port'],
           index=['ticker'],
           values=['pnl', 'ret'],
           aggregate_function='sum')
)


#
#
# with pl.Config(tbl_cols=15):
#     print(tp.blot_log.get())


with pl.Config(tbl_cols=13):
    print(tp.q_equity.get())

