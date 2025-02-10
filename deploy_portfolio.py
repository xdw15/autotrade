import sys

from libs.auto_portfolio import ToyPortfolio
import polars as pl
# from libs.auxiliary_funs import sit_qs
import datetime as dt

fecha_now = dt.datetime.now().strftime('%Y%m%d%H%M%S')

pos_0 = {
    'cash': pl.DataFrame(data=[[1e5, 1e5, 'USD']],
                         schema=['amount', 'cost_basis', 'ccy'],
                         orient='row'),
    'equity': pl.DataFrame(data=[[10.0, 10*235.2, 'USD', 'AAPL'],
                                 [30.0, 30*521.7, 'USD', 'QQQ']],
                           schema=['amount', 'cost_basis', 'ccy', 'ticker'],
                           orient='row')
}

tp = ToyPortfolio(pos_0, fecha_now)

tp.positions['equity']

