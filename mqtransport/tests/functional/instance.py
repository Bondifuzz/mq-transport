from __future__ import annotations
from typing import TYPE_CHECKING, Optional

from mqtransport import SQSApp
from .settings import AppSettings, load_app_settings

if TYPE_CHECKING:
    from mqtransport import MQApp
    from mqtransport.channel import ConsumingChannel, ProducingChannel
    from .main.participants import MP_Normal


class Channels:
    ich1: ConsumingChannel
    och1: ProducingChannel


class Producers:
    normal: MP_Normal


class MQAppState:
    channels: Channels
    producers: Producers

    def __init__(self):
        self.channels = Channels()
        self.producers = Producers()


class MQAppInitializerEmpty:

    _settings: AppSettings
    _app: Optional[MQApp]

    @property
    def app(self) -> MQApp:
        assert self._app is not None
        return self._app

    def __init__(self, settings: AppSettings):
        self._settings = settings
        self._app = None

    async def do_init(self):

        self._app = await self._create_mq_app()
        self._app.state = MQAppState()

        try:
            await self._app.ping()
        except:
            await self._app.shutdown()
            raise

    async def _create_mq_app(self):

        mq_broker = self._settings.message_queue.broker.lower()
        mq_settings = self._settings.message_queue

        if mq_broker == "sqs":
            app = await SQSApp.create(
                mq_settings.username,
                mq_settings.password,
                mq_settings.region,
                mq_settings.url,
            )
        else:
            raise ValueError(f"Unsupported message broker: {mq_broker}")

        return app


class MQAppInitializer(MQAppInitializerEmpty):

    _settings: AppSettings
    _app: MQApp

    async def do_init(self):

        self._app = await self._create_mq_app()
        self._app.state = MQAppState()

        try:
            await self._app.ping()
            await self._configure_channels()

        except:
            await self._app.shutdown()
            raise

    async def _configure_channels(self):

        state: MQAppState = self._app.state
        queues = self._settings.message_queue.queues

        ich1 = await self._app.create_consuming_channel(queues.test1)
        och1 = await self._app.create_producing_channel(queues.test1)
        dlq = await self._app.create_producing_channel(queues.dlq)

        ich1.use_dead_letter_queue(dlq)
        await ich1.purge()
        await dlq.purge()

        state.channels.ich1 = ich1
        state.channels.och1 = och1


async def create_mq_instance_empty(settings):
    initializer = MQAppInitializerEmpty(settings)
    await initializer.do_init()
    return initializer.app


async def create_mq_instance(settings):
    initializer = MQAppInitializer(settings)
    await initializer.do_init()
    return initializer.app


async def create_mq_instance_with_auth_override(
    endpoint_url=None, username=None, password=None
):

    settings = load_app_settings()

    if endpoint_url:
        settings.message_queue.url = endpoint_url
    if username:
        settings.message_queue.username = username
    if password:
        settings.message_queue.password = password

    initializer = MQAppInitializerEmpty(settings)
    await initializer.do_init()
    return initializer.app
