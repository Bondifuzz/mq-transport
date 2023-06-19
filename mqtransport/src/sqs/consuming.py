from __future__ import annotations
from typing import TYPE_CHECKING
from contextlib import suppress
from logging import Logger
import asyncio

from typing import (
    AsyncIterator,
    Optional,
    List,
    Dict,
)

from pydantic import ValidationError
from botocore.exceptions import HTTPClientError

from ..base.channel import ConsumingChannel
from ..base.participants import Consumer, MC_AccessCheck
from ..base.errors import ConsumeMessageError, ChannelError
from ..base.util import delay

from .producing import SQSProducingChannel
from .pack import unpack_message, MessageUnpackError
from .util import wrap_botocore_errors


if TYPE_CHECKING:
    from .app import SQSApp
    from aioboto3_hints.sqs.client import Client as SQSClient
else:
    SQSClient = object


class SQSMessageReceiver:

    _queue_url: str
    _client: SQSClient
    _messages: List[dict]

    def __init__(self, queue_url: str, client: SQSClient):
        self._queue_url = queue_url
        self._client = client
        self._messages = []

    def __aiter__(self):
        return self

    async def __anext__(self):

        if not self._messages:
            self._messages = await self._receive_messages()
            self._messages.reverse()

        return self._messages.pop()

    async def _receive_messages(self) -> List[dict]:

        messages = None
        while not messages:

            try:
                resp: dict = await self._client.receive_message(
                    QueueUrl=self._queue_url,
                    MessageAttributeNames=["All"],
                    MaxNumberOfMessages=10,
                )

            except HTTPClientError as e:

                #
                # aioboto3 wraps unhandled exceptions into HTTPClientError
                # We need to restore CancelledError if it has been raised
                #

                inner_e = e.kwargs.get("error")
                if isinstance(inner_e, asyncio.CancelledError):
                    raise inner_e

                raise

            messages = resp.get("Messages")

        return messages


class SQSConsumingChannel(ConsumingChannel):

    _client: SQSClient
    _consumers: Dict[str, Consumer]
    _dlq: Optional[SQSProducingChannel]
    _consuming_task: Optional[asyncio.Task]
    _consume_lock: asyncio.Lock
    _queue_name: str
    _queue_url: str
    _logger: Logger
    _app: SQSApp

    @wrap_botocore_errors
    async def purge(self):
        await self._client.purge_queue(QueueUrl=self._queue_url)

    @property
    def app(self):
        return self._app

    @property
    def name(self):
        return self._queue_name

    @property
    def consumers(self):
        return list(self._consumers.values())

    @wrap_botocore_errors
    async def _check_message_queue(self):

        qname = self._queue_name
        self._logger.info("Check message queue '%s' exists", qname)
        resp = await self._client.get_queue_url(QueueName=qname)
        self._queue_url = resp["QueueUrl"]

        self._logger.info("Check read permissions for queue '%s'", qname)
        await self._client.receive_message(
            QueueUrl=self._queue_url,
            MessageAttributeNames=["NoSuchTopic"],
            WaitTimeSeconds=0,
        )

    async def _init(
        self,
        queue_name: str,
        client: SQSClient,
        app: SQSApp,
    ):
        self._app = app
        self._client = client
        self._queue_name = queue_name
        self._dlq = None

        self._logger = self.get_logger()
        self._consume_lock = asyncio.Lock()
        self._consuming_task = None
        self._consumers = dict()

        await self._check_message_queue()
        self.add_consumer(MC_AccessCheck())

    @staticmethod
    async def create(
        queue_name: str,
        client: SQSClient,
        app: SQSApp,
    ):
        _self = SQSConsumingChannel()
        await _self._init(queue_name, client, app)
        return _self

    def use_dead_letter_queue(self, channel: SQSProducingChannel):
        assert isinstance(channel, SQSProducingChannel)
        self._dlq = channel

    def _find_consumer(self, name: str):
        try:
            consumer = self._consumers[name]
        except KeyError as e:
            err = f"Consumer for message '{name}' was not found"
            self._logger.error("%s in channel '%s'", err, self.name)
            raise ConsumeMessageError(err) from e

        return consumer

    async def _process_message(self, name: str, body: dict):

        try:
            consumer = self._find_consumer(name)
            validated_body = consumer.Model.parse_obj(body)
            await consumer.consume(validated_body, self._app)

        except ValidationError as e:
            err = f"Validation of message '{name}' failed"
            self._logger.error("%s in channel '%s'", err, self.name)
            self._logger.debug("Verbose error output:\n%s", str(e))
            raise ConsumeMessageError(err) from e

    async def _confirm_message_processed(self, msg: dict):
        await self._client.delete_message(
            ReceiptHandle=msg["ReceiptHandle"],
            QueueUrl=self._queue_url,
        )

    async def _send_message_to_dead_letter_queue(self, msg: dict):

        msg = {
            "MessageBody": msg.get("Body", {}),
            "MessageAttributes": msg.get("MessageAttributes", {}),
        }

        await self._dlq.send_message_raw(msg)

    async def _process_raw_message(self, message: dict):

        try:
            name, body = unpack_message(message)
            text = "Processing message '%s' in channel '%s'"
            self._logger.debug(text, name, self.name)

            try:
                await self._process_message(name, body)
                await self._confirm_message_processed(message)
                self._logger.debug("Processing message '%s' - OK", name)

            except ConsumeMessageError:

                self._logger.error("Processing message '%s' - FAILED", name)

                if not self._dlq:
                    return

                await self._confirm_message_processed(message)
                await self._send_message_to_dead_letter_queue(message)
                self._logger.error("Message '%s' is moved to DLQ", name)

            except:
                self._logger.exception("Processing message '%s' - FAILED", name)

        except MessageUnpackError as e:

            err = "Malformed message received in channel '%s'"
            self._logger.error(err, self.name)

            if not self._dlq:
                return

            await self._confirm_message_processed(message)
            await self._send_message_to_dead_letter_queue(message)

            err = "Malformed message is moved to DLQ ('%s'->'%s')"
            self._logger.error(err, self.name, self._dlq.name)

    def _receive_messages(self) -> AsyncIterator[dict]:
        return SQSMessageReceiver(self._queue_url, self._client)

    @wrap_botocore_errors
    async def _consume_from_channel(self):
        async for msg in self._receive_messages():
            async with self._consume_lock:
                await self._process_raw_message(msg)

    async def _consume_from_channel_loop(self):

        cname = self._queue_name
        self._logger.info("Receiving messages in channel '%s'", cname)

        while True:
            try:
                await self._consume_from_channel()
            except asyncio.CancelledError:
                break  # Graceful shutdown
            except ChannelError as e:
                self._logger.error(str(e))
                await delay()
            except:
                self._logger.exception(f"Unhandled error in channel '{cname}'")
                await delay()

    async def start_consuming(self):

        assert self._consuming_task is None, "Task already started"
        assert len(self._consumers) > 1, "At least one consumer must be present"

        self._logger.info(f"Start consuming in channel '{self.name}'")
        self._consuming_task = asyncio.get_running_loop().create_task(
            self._consume_from_channel_loop()
        )

    async def wait_for_stop(self):
        with suppress(asyncio.CancelledError):
            await self._consuming_task

    async def stop_consuming(self):

        assert self._consuming_task is not None, "Task not started"
        self._logger.info(f"Stop consuming in channel '{self.name}'")

        async with self._consume_lock:
            self._consuming_task.cancel()
            await self.wait_for_stop()

        self._consuming_task = None
        self._logger.info(f"Stop consuming in channel '{self.name}' - OK")

    def _is_consumer_registered(self, consumer: Consumer):
        return consumer.name in self._consumers.keys()

    def add_consumer(self, consumer: Consumer):

        assert isinstance(consumer, Consumer)
        assert not self._is_consumer_registered(consumer)

        self._logger.info(f"Added consumer '{consumer.name}' to channel '{self.name}'")
        self._consumers[consumer.name] = consumer
