import numpy as np
import polars as pl
from abc import ABC, abstractmethod


class AutoPort(ABC):

    """
    auto port acts as a single application to
    represent an actual self trading portfolio.
    it relies on other applications to support
    its methods
    """

    @abstractmethod
    def riskmanager(self):
        """
        the 7uck1ng brain behind managing the system
        :return:
        """
        pass

    # the following methods can be used withing the scope of the application
    @abstractmethod
    def _init_composition(self) -> dict:
        """
        initialize the portfolio positions to match the app framework
        :return:
        """
        pass

    @abstractmethod
    def composition(self):
        """
        returns the composition of the portfolio
        :return:
        """
        pass

    @abstractmethod
    def trade_blot(self):
        """
        process all trading order - related information
        :return:
        """
        pass

    # these methods rely on communication with other applications

    @abstractmethod
    def client_datahandler(self):
        """
        establishes connection to interact with the datahandler app
        :return:
        """
        pass

    @abstractmethod
    def client_executionhandler(self):
        """
        establishes connection with the execuionhandler app
        :return:
        """
        pass

    @abstractmethod
    def endpoint_strategies(self):
        """
        process requests coming from strategy apps
        :return:
        """
        pass

    # these methods offer calculations for reporting/monitoring purposes

    @abstractmethod
    def report_pnl(self):
        pass


import pika
import datetime as dt


class ToyPortfolio:
    """
    a portfolio implementation that can deal with equities at first
    """

    def __init__(
            self,
            initial_holdings: dict,
            time_stamp: dt.datetime,
            price_ccy: str = None,
    ):

        self.positions = self._init_composition(
            initial_holdings,
            time_stamp
        )

        self.price_ccy = price_ccy or 'USD'

    def _init_composition(self,
                          p0: dict,
                          time_stamp: dt.datetime
    ) -> dict:
        """
        :param p0: a dict with each key containing
        a type of security with positions vector
        :return:
        """

        # supported securities and required fields per security
        supported_securities = {
            'Equity': {
                'Ccy': pl.String,
                'Amount': pl.Float64,
                'Ticker': pl.String,
            },
            'Cash': {
                'Ccy': pl.String,
                'Amount': pl.Float64,
            }
        }

        # ensuring the securities provided are supported
        if not all(
            [True if i in supported_securities else False for i in p0.keys()]
        ):
            raise Exception('Security type is not currently supported')

        # ensuring the given securities have the necessary fields
        for security in supported_securities.keys():
            present_fields = [
                True
                if (i,j) in supported_securities[security].items()
                else False
                for i,j in zip(p0[security].columns, p0[security].dtypes)
            ]

            if not all(present_fields):

                missing_fields = [
                    col for col, present in zip(
                        p0[security].columns, present_fields
                    )
                    if not present
                ]
                raise Exception(
                    f'''
                        Missing fields for security: {security} \n
                        {missing_fields}
                    '''
                )

        # returning output
        positions = {}
        for security in supported_securities.keys():
            positions[security] = (
                p0[security]
                .with_columns(
                    pl.lit(time_stamp).alias('date').dt.round("1s")
                )
                .select(['date'] + p0[security].columns)
            )

        return positions


    def client_datahandler(self):











