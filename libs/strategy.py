from abc import ABC,abstractmethod

class Strategy(ABC):
    """
    this class is intended to function as an app
    directly connected to the portfolio app
    """

    @abstractmethod
    def client_signal_postman(self):
        """
        sends signals to the portfolio app
        :return:
        """
        pass
    @abstractmethod
    def endpoint_datahandler(self):
        """
        connects to the data handler app
        :return:
        """
        pass