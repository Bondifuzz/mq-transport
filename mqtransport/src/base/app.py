from __future__ import annotations
from typing import TYPE_CHECKING

from abc import ABCMeta, abstractmethod
import logging


if TYPE_CHECKING:
    from .channel import ConsumingChannel, ProducingChannel
    from typing import List, Dict, Any, Optional


class MQApp(metaclass=ABCMeta):

    """
    Description:
        `MQApp` is the main class of `mqtransport` module.
        The `MQApp` object connects other primitives
        together and coordinates their work.
    """

    @property
    @abstractmethod
    def state(self) -> Any:
        """
        Property to store something inside:
        database connection, resources, e.t.c.
        """

    @property
    @abstractmethod
    def consuming_channels(self) -> List[ConsumingChannel]:
        """
        Read-only list of created consuming channels
        `mqtransport.channel.ConsumingChannel`
        """

    @property
    @abstractmethod
    def producing_channels(self) -> List[ProducingChannel]:
        """
        Read-only list of created producing channels
        `mqtransport.channel.ProducingChannel`
        """

    @abstractmethod
    async def start(self) -> None:
        """
        Starts `MQApp`. When app is running,
        consumers are processing messages
        and producers are producing messages
        """

    @abstractmethod
    async def run_forever(self) -> None:
        """
        The blocking version of `start` method,
        which infinitely waits for `stop` call
        """

    @abstractmethod
    async def stop(self, timeout: Optional[int] = None) -> None:
        """
        Stops `MQApp`. When app is stopped,
        consumers are not processing messages
        and producers are not producing messages.
        When stopped,`MQApp` can be started again
        calling `start` method
        """

    @abstractmethod
    async def shutdown(self, timeout: Optional[int] = None) -> None:
        """
        The same as `stop` call, hovewer performs full cleanup.
        Will not longer able to call `start` method
        """

    @abstractmethod
    async def ping(self) -> None:
        """
        Ensures that message broker is available
        Raises exception if it is not

        Raises:
            `mqtransport.errors.ChannelError`
        """

    @abstractmethod
    async def create_producing_channel(self, queue_name: str) -> ProducingChannel:
        """
        Creates new channel to place messages into message queue.
        Verifies write permissions by placing empty message into queue.

        Raises exception if:
        <ol>
            <li>If message queue with provided name does not exist</li>
            <li>Not enough permissions to place message into queue</li>
        </ol>

        Args:
            queue_name (str): The name of message queue

        Returns:
            `mqtransport.channel.ProducingChannel`: Created channel

        Raises:
            `mqtransport.errors.ChannelError`
        """

    @abstractmethod
    async def create_consuming_channel(self, queue_name: str) -> ConsumingChannel:
        """
        Creates new channel to get messages from message queue.
        Verifies read permissions by reading message with non-existent topic.

        Raises exception if:
        <ol>
            <li>If message queue with provided name does not exist</li>
            <li>Not enough permissions to get message from queue</li>
        </ol>

        Args:
            queue_name (str): The name of message queue

        Returns:
            `mqtransport.channel.ConsumingChannel`: Created channel

        Raises:
            `mqtransport.errors.ChannelError`
        """

    @abstractmethod
    def import_unsent_messages(self, unsent_messages: Dict[str, list]):
        """
        Imports previously unsent messages for further sending.
        Raises exception if import operation failed for some reason

        Args:
            unsent_messages (Dict[str, list]): Unsent messages of each channel

        Raises:
            `mqtransport.errors.ImportMessageError`
        """

    @abstractmethod
    def export_unsent_messages(self) -> Dict[str, list]:
        """
        Exports messages which haven't been sent yet.
        Return value represents a dictionary with keys containing
        channel names and values containing a list of unsent messages

        Returns:
            Dict[str, list]: Unsent messages of each channel
        """

    @staticmethod
    def get_logger():
        """ Get logger object for `MQApp` """
        return logging.getLogger("mq.app")
