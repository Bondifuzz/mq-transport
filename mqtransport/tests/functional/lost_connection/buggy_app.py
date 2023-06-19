from abc import ABCMeta, abstractmethod
from typing import AsyncContextManager
from mqtransport import MQApp


class BuggyMQApp(MQApp, metaclass=ABCMeta):
    @abstractmethod
    def lose_connection(self) -> AsyncContextManager:
        pass
