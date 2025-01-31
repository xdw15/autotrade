import sys

from libs.auto_portfolio import ToyPortfolio
import polars as pl
#from libs.auxiliary_funs import sit_qs
import datetime as dt

fecha_now = dt.datetime.now()
pos_0 = {
    'Cash': pl.DataFrame( data=[[1e5,'USD']],
                          schema=['Amount', 'Ccy'],
                          orient='row'),
    'Equity': pl.DataFrame( data=
                            [ [10.0,'USD','AAPL'],
                              [30.0, 'USD', 'QQQ']
                            ],
                            schema=['Amount', 'Ccy','Ticker'],
                            orient='row')
}

tp = ToyPortfolio(pos_0,fecha_now)


tp.positions['Equity']
