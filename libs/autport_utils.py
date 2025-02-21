import numpy as np
import polars as np
import datetime as dt

from libs.config import work_path
from libs.auxiliary_funs import *

# <editor-fold desc="table construction">
query_sit = sit_qs('20250124')

mock_port = (
    query_sit
    # .filter(pl.col('Portafolio') == 2)
    .filter(pl.col('TipoRenta') == '2')
    .filter((pl.col('Name').str.contains('QQQ'))
            | pl.col('Name').str.contains('SPY'))
    .select('Cantidad', 'Portafolio',
            'Name', 'VPNOriginal')
    .with_columns((pl.col('VPNOriginal')/pl.col('Cantidad'))
                  .alias('cost_basis'),
                  pl.lit('USD').alias('ccy'),
                  pl.col('Name').replace({'QQQ INVESCO': 'QQQ'}),
                  pl.lit(dt.datetime(2025, 1, 24)).alias('timestamp')
                  )
    .rename(
        {'Cantidad': 'amount',
         'Portafolio': 'port',
         'Name': 'ticker'}
    )
    .select(['timestamp', 'port', 'amount', 'cost_basis', 'ccy', 'ticker'])
)

mock_port.write_parquet(work_path +
                        '/synthetic_server_path/auto_port/positions.parquet')
data = pl.read_parquet(work_path + '/synthetic_server_path/us_equity.parquet')
# </editor-fold>


autoexec_db = pl.read_parquet(work_path +
                             '/synthetic_server_path/auto_exec/fill_record.parquet')

autoexec_db.columns

