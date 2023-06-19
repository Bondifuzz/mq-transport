from __future__ import annotations
from typing import List, TYPE_CHECKING

from abc import abstractmethod, ABCMeta
import logging

if TYPE_CHECKING:
    from .participants import Producer, Consumer
    from .app import MQApp

from .util import testing_only


class ConsumingChannel(metaclass=ABCMeta):

    """
    Logical abstraction allowing to get messages
    from message queue and pass it to consumers
    """

    @property
    @abstractmethod
    def app(self) -> MQApp:
        """Reference to `mqtransport.MQApp` object"""

    @property
    @abstractmethod
    def name(self) -> str:
        """Channel name. The same as queue name it linked to"""

    @property
    @abstractmethod
    def consumers(self) -> List[Consumer]:
        """List of consumers, added to this channel"""

    @abstractmethod
    def add_consumer(self, consumer: Consumer) -> None:
        """
        Adds new consumer to channel

        Args:
            consumer (Consumer): consumer to add
        """

    @abstractmethod
    async def start_consuming(self) -> None:
        """
        Starts receiving messages from message queue.
        Passes received message to one of registered consumers,
        if message is not malformed and consumer is found.
        """

    @abstractmethod
    async def stop_consuming(self) -> None:
        """ Stops receiving messages from message queue """

    @abstractmethod
    def use_dead_letter_queue(self, channel: ProducingChannel) -> None:
        """
        Adds new channel, linked to dead letter queue.
        When received message is malformed or consumer
        for this message is not found, it will be moved
        straight into dead letter queue using this channel.

        Args:
            channel (ProducingChannel): forwarder to DLQ
        """

    @abstractmethod
    async def purge(self) -> None:
        """
        Remove all messages from message queue this channel linked.

        Raises:
            `mqtransport.errors.ChannelError`
        """

    @staticmethod
    def get_logger() -> logging.Logger:
        """Get logger object for `ConsumingChannel`"""
        return logging.getLogger("mq.channel.in")


class ProducingChannel(metaclass=ABCMeta):

    """
    Logical abstraction allowing to place messages into
    message queue and guaranteeing that outcoming
    message will definitely be delivered
    """

    @property
    @abstractmethod
    def app(self) -> MQApp:
        """Reference to `mqtransport.MQApp` object"""

    @property
    @abstractmethod
    def name(self) -> str:
        """Channel name. The same as queue name it linked to"""

    @property
    @abstractmethod
    def producers(self) -> List[Producer]:
        """List of producers, added to this channel"""

    @abstractmethod
    async def add_producer(self, producer: Producer) -> None:
        """
        Adds new producer to channel

        Args:
            producer (Producer): producer to add
        """

    @abstractmethod
    async def send_message(self, name: str, body: dict) -> None:
        """
        Packs and sends new message to message queue.

        Args:
            name (str): Unique name of consumer which will receive the message
            body (dict): Message body in `<key,value>` form
        """

    @abstractmethod
    async def start_producing(self) -> None:
        """
        Starts sending messages to message queue.
        Sends messages until succeeded. Performs send
        retries if message broker becomes unavailable
        """

    @abstractmethod
    async def stop_producing(self, force: bool) -> None:
        """
        Stops sending messages to message queue.
        If `force` flag is set, stops immediately,
        potentially losing messages, which were
        not delivered to message broker.  If `force`
        flag is not set, waits until all unsent
        messages will be delivered to message broker.

        Args:
            force (bool): Whether to stop immediately
        """

    @abstractmethod
    @testing_only
    async def purge(self) -> None:
        """
        **WARNING**: Can be called only during testing.

        Remove all messages from message queue this channel linked.

        Raises:
            `mqtransport.errors.ChannelError`
        """

    @staticmethod
    def get_logger() -> logging.Logger:
        """Get logger object for `ProducingChannel`"""
        return logging.getLogger("mq.channel.out")
