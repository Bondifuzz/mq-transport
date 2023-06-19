from __future__ import annotations
from typing import TYPE_CHECKING, List, Dict, Any, Optional
from contextlib import AsyncExitStack, suppress
import asyncio

try:
    import aioboto3
except ModuleNotFoundError:
    raise Exception('Please, install "aioboto3 8.3.x or 9.x" package to use SQSApp')

from botocore.exceptions import ClientError
from aiohttp.client_exceptions import ClientConnectionError

from .consuming import SQSConsumingChannel
from .producing import SQSProducingChannel

from ..base.errors import ChannelError
from ..base.app import MQApp


if TYPE_CHECKING:
    from aioboto3_hints.sqs.client import Client as SQSClient
else:
    SQSClient = object


class SQSApp(MQApp):

    _client: SQSClient
    _consuming_channels: List[SQSConsumingChannel]
    _producing_channels: List[SQSProducingChannel]
    _context_stack: Optional[AsyncExitStack]
    _is_running: bool
    _is_closed: bool
    _state: Any

    @staticmethod
    async def _create_client(**kwargs):

        try:
            # aioboto 9.0.0+
            session = aioboto3.Session()
            client = session.client(**kwargs)

        except AttributeError:
            # aioboto 8.X.X and older
            client = aioboto3.client(**kwargs)

        context_stack = AsyncExitStack()
        client = await context_stack.enter_async_context(client)

        return client, context_stack

    @property
    def consuming_channels(self):
        return self._consuming_channels

    @property
    def producing_channels(self):
        return self._producing_channels

    @property
    def state(self) -> Any:
        return self._state

    @state.setter
    def state(self, value: Any):
        self._state = value

    async def _init(
        self,
        username: str,
        password: str,
        region_name: str,
        endpoint_url: Optional[str],
    ):
        self._logger = self.get_logger()
        self._consuming_channels = list()
        self._producing_channels = list()
        self._context_stack = None
        self._is_running = False
        self._is_closed = True
        self._state = None

        client, stack = await SQSApp._create_client(
            service_name="sqs",
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=username,
            aws_secret_access_key=password,
        )

        self._client = client
        self._context_stack = stack
        self._is_closed = False

    @staticmethod
    async def create(
        username: str,
        password: str,
        region_name: str,
        endpoint_url: Optional[str] = None,
    ):
        _self = SQSApp()
        await _self._init(
            username,
            password,
            region_name,
            endpoint_url,
        )
        return _self

    async def ping(self):
        try:
            await self._client.list_queues(MaxResults=1)
        except (ClientConnectionError, ClientError) as e:
            raise ChannelError(str(e)) from e

    def _consuming_channel_exists(self, name: str):
        return name in list(map(lambda c: c.name, self._consuming_channels))

    def _producing_channel_exists(self, name: str):
        return name in list(map(lambda c: c.name, self._producing_channels))

    async def create_consuming_channel(self, name: str) -> SQSConsumingChannel:
        assert not self._consuming_channel_exists(name), "Channel already exists"
        channel = await SQSConsumingChannel.create(name, self._client, self)
        self._consuming_channels.append(channel)
        return channel

    async def create_producing_channel(self, name: str) -> SQSProducingChannel:
        assert not self._producing_channel_exists(name), "Channel already exists"
        channel = await SQSProducingChannel.create(name, self._client, self)
        self._producing_channels.append(channel)
        return channel

    async def _start_consuming(self):

        self._logger.info(f"Starting consuming channels...")

        for channel in self._consuming_channels:
            await channel.start_consuming()

        self._logger.info(f"Starting consuming channels... OK")

    async def _stop_consuming(self):

        self._logger.info(f"Stopping consuming channels...")

        for channel in self._consuming_channels:
            await channel.stop_consuming()

        self._logger.info(f"Stopping consuming channels... OK")

    async def _start_producing(self):

        self._logger.info(f"Starting producing channels...")

        for channel in self._producing_channels:

            if len(channel.producers) == 0:
                msg = "No producers were registered in channel '%s'"
                self._logger.warning(msg, channel.name)

            await channel.start_producing()

        self._logger.info(f"Starting producing channels... OK")

    async def _stop_producing_channel(
        self, channel: SQSProducingChannel, timeout: Optional[int]
    ):
        try:
            coro = channel.stop_producing(force=False)
            await asyncio.wait_for(coro, timeout)

        except asyncio.TimeoutError:
            await channel.stop_producing(force=True)

    async def _stop_producing(self, timeout: Optional[int] = None):

        self._logger.info(f"Stopping producing channels...")

        coros = []
        for channel in self._producing_channels:
            coros.append(self._stop_producing_channel(channel, timeout))

        await asyncio.gather(*coros)
        self._logger.info(f"Stopping producing channels... OK")

    def import_unsent_messages(self, unsent_messages: Dict[str, list]):

        self._logger.info(f"Importing unsent messages...")
        msg = "Imported %d unsent messages to channel '%s'"

        for channel in self._producing_channels:
            with suppress(KeyError):
                messages = unsent_messages[channel.name]
                channel.import_unsent_messages(messages)
                self._logger.debug(msg, len(messages), channel.name)

        self._logger.info(f"Importing unsent messages... OK")

    def export_unsent_messages(self) -> Dict[str, list]:

        self._logger.info(f"Exporting unsent messages...")
        msg = "Exported %d unsent messages from channel '%s'"

        unsent_messages = dict()
        for channel in self._producing_channels:
            messages = channel.export_unsent_messages()
            unsent_messages[channel.name] = messages
            self._logger.debug(msg, len(messages), channel.name)

        self._logger.info(f"Exporting unsent messages... OK")

        return unsent_messages

    async def _close(self):

        assert not self._is_closed, "App has already been closed"

        if self._context_stack:
            await self._context_stack.aclose()
            self._context_stack = None

        self._is_closed = True

    def __del__(self):
        if not self._is_closed:
            self._logger.error(f"Unclosed MQ connection")

    async def start(self):

        assert (
            self._producing_channels or self._consuming_channels
        ), "At least one producing or consuming channel must be used"
        assert not self._is_running, "App has already been started"
        assert not self._is_closed, "Unable to start app after shutdown"

        self._logger.info("Starting app...")

        await self._start_consuming()
        await self._start_producing()
        self._is_running = True

        self._logger.info("Starting app... OK")

    async def run_forever(self):

        assert self._consuming_channels, "At least one consuming channel must be used"

        if not self._is_running:
            await self.start()

        awaitables = []
        for channel in self._consuming_channels:
            awaitables.append(channel.wait_for_stop())
        for channel in self._producing_channels:
            awaitables.append(channel.wait_for_stop())

        self._logger.info("Waiting for app stop or shutdown...")
        await asyncio.wait(awaitables)

    async def stop(self, timeout: Optional[int] = None):

        assert self._is_running, "App has not been started"

        self._logger.info("Stopping app...")

        await self._stop_consuming()
        await self._stop_producing(timeout)
        self._is_running = False

        self._logger.info("Stopping app... OK")

    async def shutdown(self, timeout: Optional[int] = None):

        self._logger.info("Shutdown app...")

        if self._is_running:
            await self.stop(timeout)

        await self._close()
        self._logger.info("Shutdown app... OK")
