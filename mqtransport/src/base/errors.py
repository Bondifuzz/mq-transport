class MQTransportError(Exception):
    """Base exception for all mq-transport errors"""


class ChannelError(MQTransportError):
    """Thrown on failures, which don't allow to work with message queues"""


class ConsumeMessageError(MQTransportError):
    """
    Exceptions for all situations when message can not be consumed.
    This message will be forwarded to dead letter queue.
    """


class ImportMessageError(MQTransportError):
    """Thrown when message import failed"""
