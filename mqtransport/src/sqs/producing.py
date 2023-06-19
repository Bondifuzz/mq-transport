from __future__ import annotations
from typing import TYPE_CHECKING, List, Optional
from contextlib import suppress
from logging import Logger
from copy import copy
import asyncio

from ..base.participants import MC_AccessCheck
from ..base.channel import ProducingChannel
from ..base.participants import Producer
from ..base.errors import ChannelError, ImportMessageError
from ..base.util import testing_only, delay

from .util import wrap_botocore_errors
from .pack import MessagePackError, pack_message, unpack_message


if TYPE_CHECKING:
    from .app import SQSApp
    from aioboto3_hints.sqs.client import Client as SQSClient
else:
    SQSClient = object


class SQSProducingChannel(ProducingChannel):

    _max_msg_size: int
    _producing_task: asyncio.Task
    _backlog_queue: asyncio.Queue
    _current_message: Optional[dict]
    _producers: List[Producer]
    _client: SQSClient
    _queue_name: str
    _queue_url: str
    _logger: Logger
    _app: SQSApp

    @property
    def app(self):
        return self._app

    @property
    def name(self):
        return self._queue_name

    @property
    def producers(self):
        return self._producers

    @testing_only
    @wrap_botocore_errors
    async def purge(self):
        await self._client.purge_queue(QueueUrl=self._queue_url)

    @wrap_botocore_errors
    async def _check_message_queue(self):

        qname = self._queue_name
        self._logger.info("Checking message queue '%s' exists", qname)
        resp = await self._client.get_queue_url(QueueName=qname)
        self._queue_url = resp["QueueUrl"]

        message = pack_message(MC_AccessCheck.name, {}, 2)
        self._logger.info("Checking write permissions for queue '%s'", qname)
        await self._client.send_message(QueueUrl=self._queue_url, **message)

        resp = await self._client.get_queue_attributes(
            QueueUrl=self._queue_url, AttributeNames=["MaximumMessageSize"]
        )

        try:
            self._max_msg_size = int(resp["Attributes"]["MaximumMessageSize"])
        except KeyError:
            # Not implemented in ElasticMQ -> set default for AWS SQS
            self._max_msg_size = 262144

    async def _init(
        self,
        channel_name: str,
        client: SQSClient,
        app: SQSApp,
    ):
        self._app = app
        self._client = client
        self._queue_name = channel_name

        self._logger = self.get_logger()
        self._backlog_queue = asyncio.Queue()
        self._current_message = None
        self._producing_task = None
        self._producers = list()

        await self._check_message_queue()

    @staticmethod
    async def create(
        channel_name: str,
        client: SQSClient,
        app: SQSApp,
    ):
        _self = SQSProducingChannel()
        await _self._init(channel_name, client, app)
        return _self

    def _is_producer_registered(self, producer: Producer):
        return producer.name in list(map(lambda p: p.name, self._producers))

    def add_producer(self, producer: Producer):

        assert isinstance(producer, Producer)
        assert not self._is_producer_registered(producer)

        producer.channel = self
        self._producers.append(producer)

        msg = "Added producer '%s' to channel '%s'"
        self._logger.info(msg, producer.name, self.name)

    @wrap_botocore_errors
    async def _send_message_internal(self, message: dict):
        await self._client.send_message(QueueUrl=self._queue_url, **message)

    async def send_message_raw(self, message: dict):
        await self._backlog_queue.put(message)

    async def send_message(self, name: str, body: dict):
        message = pack_message(name, body, self._max_msg_size)
        await self._backlog_queue.put(message)

    async def _send_until_succeeded(self, message: dict):

        succeeded = False
        while not succeeded:
            try:
                await self._send_message_internal(message)
                succeeded = True

            except ChannelError as e:
                messages_unsent = self._backlog_queue.qsize() + 1
                self._logger.error("Error in channel '%s' - %s", self.name, e)
                self._logger.warning("Messages unsent: %d", messages_unsent)
                await delay()

    async def _get_message_to_send(self):

        if self._current_message is not None:
            return self._current_message

        message = await self._backlog_queue.get()
        self._current_message = message
        return message

    def _confirm_message_sent(self):
        self._current_message = None
        self._backlog_queue.task_done()

    async def _produce_messages_loop(self):

        while True:

            try:
                message = await self._get_message_to_send()
                await self._send_until_succeeded(message)
                self._confirm_message_sent()

            except asyncio.CancelledError:
                break  # Graceful shutdown

            except:
                self._logger.exception("Unhandled error in channel '%s'", self.name)
                self._logger.critical("Unable to continue")
                break

    def export_unsent_messages(self) -> List[dict]:

        assert self._producing_task is None, "App must not be running"

        messages = []
        queue = copy(self._backlog_queue)

        def export_msg(msg: dict):
            name, body = unpack_message(msg)
            return {"name": name, "body": body}

        if self._current_message is not None:
            messages.append(export_msg(self._current_message))

        while not queue.empty():
            messages.append(export_msg(queue.get_nowait()))

        return messages

    def import_unsent_messages(self, messages: List[dict]):

        assert self._producing_task is None, "App must not be running"

        for message in messages:

            try:
                name = message["name"]
                body = message["body"]
                size = self._max_msg_size
                packed_msg = pack_message(name, body, size)

            except KeyError as e:
                msg = f"Failed to import message - KeyError: '{e}'"
                raise ImportMessageError(msg) from e

            except MessagePackError as e:
                msg = f"Failed to pack imported message - '{e}'"
                raise ImportMessageError(msg) from e

            self._backlog_queue.put_nowait(packed_msg)

    async def start_producing(self):

        assert self._producing_task is None, "Task already started"
        self._logger.info("Start producing in channel '%s'", self.name)

        self._producing_task = asyncio.get_running_loop().create_task(
            self._produce_messages_loop()
        )

    async def wait_for_stop(self):
        with suppress(asyncio.CancelledError):
            await self._producing_task

    async def stop_producing(self, force: bool = False):

        self._logger.info("Stop producing in channel '%s'", self.name)
        assert self._producing_task is not None, "Not started"

        if not force:
            self._logger.info("Waiting until all messages will be sent")
            await self._backlog_queue.join()

        self._producing_task.cancel()
        await self.wait_for_stop()

        self._producing_task = None
        self._logger.info("Stop producing in channel '%s' - OK", self.name)
