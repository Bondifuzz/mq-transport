from __future__ import annotations
from typing import Optional

import logging
from pydantic import BaseModel

from .channel import ProducingChannel
from .app import MQApp


class Consumer:

    """
    Base class for all consumers.
    <ol>
        <li>Consumer must have unique `name`</li>
        <li>Consumer must have a `Model` defining the structure of message body</li>
        <li>Consumer must implement `consume` method to process incoming messages</li>
    </ol>
    """

    name = ""
    """ Unique name of consumer. Must not be empty """

    class Model(BaseModel):
        """
        Defines the structure of message body.
        Based on [pydantic](https://pydantic-docs.helpmanual.io)
        """

    _logger: logging.Logger

    def __init__(self):
        assert len(self.name) > 0, "Consumer name not set"
        self._logger = logging.getLogger("mq.consumer")
        self.Model.update_forward_refs()

    async def consume(self, msg: Model, app: MQApp):
        """
        Incoming message handler. Must be overridden

        Args:
            msg (Model): Validated message to process
            app (MQApp): Reference to the app object

        Raises:
            `NotImplementedError`
        """
        raise NotImplementedError()

    @property
    def logger(self):
        """Logger object for consumer"""
        return self._logger


class Producer:

    """
    Base class for all producers.
    <ol>
        <li>Producer must have unique `name`</li>
        <li>Consumer must have a `Model` defining the structure of message body</li>
    </ol>
    """

    name: str = ""
    """ Unique name of producer. Must not be empty """

    class Model(BaseModel):
        """
        Defines the structure of message body.
        Based on [pydantic](https://pydantic-docs.helpmanual.io)
        """

    _channel: Optional[ProducingChannel]
    _logger: logging.Logger

    @property
    def channel(self):
        """ Channel to send messages """
        return self._channel

    @channel.setter
    def channel(self, channel):
        assert isinstance(channel, ProducingChannel)
        self._channel = channel

    def __init__(self):
        assert len(self.name) > 0, "Producer name not set"
        self._logger = logging.getLogger("mq.producer")
        self.Model.update_forward_refs()
        self._channel = None

    async def produce(self, **kwargs):

        """
        Produce message. Message is composed from
        arbitrary values passed to `**kwargs`.
        Message is validated before send

        Args:
            kwargs (dict): Arbitrary values

        Raises:
            `pydantic.ValidationError`,
            `mqtransport.errors.ChannelError`
        """

        if __debug__:
            self.Model.parse_obj(kwargs)

        assert self._channel, "Channel must be set"
        await self._channel.send_message(self.name, kwargs)

    @property
    def logger(self):
        """Logger object for producer"""
        return self._logger


class MC_AccessCheck(Consumer):

    """Used to filter out access check messages"""

    name: str = "_access_check"

    async def consume(self, *unused):
        self._logger.debug("Got access check message. Ok")
