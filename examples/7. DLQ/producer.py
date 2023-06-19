from __future__ import annotations
from contextlib import suppress
from pydantic import BaseModel
from random import randint
import logging
import asyncio

from mqtransport import MQApp, SQSApp
from mqtransport.participants import Producer
from settings import AppSettings, load_app_settings


class MP_NoLogic(Producer):

    name: str = "messages.nologic"

    class Model(BaseModel):
        speed: int


class MP_UnhandledError(Producer):

    name: str = "messages.unhandled"

    class Model(BaseModel):
        div: int


class MP_NoSuchName(Producer):

    name: str = "messages.no.such.name"

    class Model(BaseModel):
        rnd: int


class MP_MalformedBody(Producer):

    name: str = "messages.malformed"

    class Model(BaseModel):
        x: int
        y: int


class MQAppState:
    mp_unhandled: MP_UnhandledError
    mp_malformed: MP_MalformedBody
    mp_no_name: MP_NoSuchName
    mp_no_logic: MP_NoLogic


class MQAppProduceInitializer:

    _settings: AppSettings
    _app: MQApp

    @property
    def app(self):
        return self._app

    def __init__(self, settings: AppSettings):
        self._settings = settings
        self._app = None

    async def do_init(self):

        self._app = await self._create_mq_app()
        self._app.state = MQAppState()

        try:
            await self._app.ping()
            await self._configure_channels()

        except:
            await self._app.shutdown()
            raise

    async def _create_mq_app(self):

        broker = self._settings.message_queue.broker.lower()
        settings = self._settings.message_queue

        if broker == "sqs":
            app = await SQSApp.create(
                settings.username,
                settings.password,
                settings.region,
                settings.url,
            )
        else:
            raise ValueError(f"Unsupported message broker: {broker}")

        return app

    async def _configure_channels(self):

        state: MQAppState = self._app.state
        queues = self._settings.message_queue.queues
        channel = await self._app.create_producing_channel(queues.into_dlq)

        mp_unhandled = MP_UnhandledError()
        channel.add_producer(mp_unhandled)
        state.mp_unhandled = mp_unhandled

        mp_malformed = MP_MalformedBody()
        channel.add_producer(mp_malformed)
        state.mp_malformed = mp_malformed

        mp_no_name = MP_NoSuchName()
        channel.add_producer(mp_no_name)
        state.mp_no_name = mp_no_name

        mp_no_logic = MP_NoLogic()
        channel.add_producer(mp_no_logic)
        state.mp_no_logic = mp_no_logic


async def create_mq_instance():
    settings = load_app_settings()
    initializer = MQAppProduceInitializer(settings)
    await initializer.do_init()
    return initializer.app


async def producing_loop(mq_app: MQApp):

    state: MQAppState = mq_app.state
    mp_unhandled = state.mp_unhandled
    mp_malformed = state.mp_malformed
    mp_no_logic = state.mp_no_logic
    mp_no_name = state.mp_no_name

    logging.info("Producing bad messages...")
    await mp_no_logic.produce(speed=-1)
    await asyncio.sleep(3)
    await mp_malformed.produce(x=1, y=2)
    await asyncio.sleep(3)
    await mp_no_name.produce(rnd=3)
    await asyncio.sleep(3)
    await mp_unhandled.produce(div=0)
    await asyncio.sleep(3)


if __name__ == "__main__":

    #
    # Setup logging. Make some loggers silent to avoid mess
    #

    fmt = "%(asctime)s %(levelname)-8s %(name)-15s %(message)s"
    logging.basicConfig(format=fmt, level=logging.DEBUG)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)

    #
    # Start application
    # We need loop to start app coroutine
    #

    loop = asyncio.get_event_loop()
    logging.info("Creating MQApp")
    mq_app = loop.run_until_complete(create_mq_instance())

    try:
        logging.info("Running MQApp. Press Ctrl+C to exit")
        loop.run_until_complete(mq_app.start())
        loop.run_until_complete(producing_loop(mq_app))

    except KeyboardInterrupt as e:
        logging.warning("KeyboardInterrupt received")

    finally:
        logging.info("Shutting MQApp down")
        loop.run_until_complete(mq_app.shutdown())
