from abc import ABC,abstractmethod


class ExecutionHandlerApp(ABC):
    """
    the class implements the Application
    that acts as the trader component of
    the architecture
    """

    @abstractmethod
    def client_send_order(self):
        """
        send portfolio orders to brokers/dealers APIs
        :return:
        """

    @abstractmethod
    def client_execution_report(self):
        """
        sends an execution report to the portfolio app after
        receiving confirmation from the broker/dealer API
        :return:
        """
    @abstractmethod
    def client_order_status(self):
        """
        sends a status request for a placed order to the
        endpoint api processing it
        :return:
        """

    @abstractmethod
    def endpoint_portfolio_orders(self):
        """
        api endpoint for requests coming from the portfolio app
        :return:
        """


