from __future__ import annotations
from pydantic import BaseModel
from random import randint
import logging
import asyncio

from mqtransport import MQApp, SQSApp
from mqtransport.participants import Producer, Consumer
from settings import AppSettings, load_app_settings


class MP_Chain(Producer):

    name: str = "messages.two"

    class Model(BaseModel):
        rnd: int


class MQAppState:
    mp_two: MP_Chain


class MC_Chain(Consumer):

    name: str = "messages.one"

    class Model(BaseModel):
        rnd: int

    async def consume(self, msg: Model, mq_app: MQApp):

        self._logger.info("[ONE->TWO] Consumed message: rnd=%d", msg.rnd)

        rand_num = randint(100, 200)
        state: MQAppState = mq_app.state
        mp_two = state.mp_two

        await mp_two.produce(rnd=rand_num)
        mp_two.logger.info("[TWO->THREE] Producing: rnd=%d", rand_num)


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

        #
        # Create channel to read from queue 'one-two'
        # Add consumer to this channel
        #

        ich1 = await self._app.create_consuming_channel(queues.one_two)
        ich1.add_consumer(MC_Chain())

        #
        # Create channel to write to queue 'two-three'
        # Add producer to this channel
        #

        mp_two = MP_Chain()
        och1 = await self._app.create_producing_channel(queues.two_three)
        och1.add_producer(mp_two)
        state.mp_two = mp_two


async def create_mq_instance():
    settings = load_app_settings()
    initializer = MQAppProduceInitializer(settings)
    await initializer.do_init()
    return initializer.app


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
        loop.run_until_complete(mq_app.run_forever())

    except KeyboardInterrupt as e:
        logging.warning("KeyboardInterrupt received")

    finally:
        logging.info("Shutting MQApp down")
        loop.run_until_complete(mq_app.shutdown())
