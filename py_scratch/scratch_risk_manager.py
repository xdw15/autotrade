from libs.auto_portfolio import ToyPortfolio

class AutoRiskManager:
    def __init__(self, auto_portfolio):
        self._positions = auto_portfolio.positions

    def trade_overhead(self, order):

        self._positions['cash']
