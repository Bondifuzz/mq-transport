from __future__ import annotations
from contextlib import suppress
from pydantic import BaseModel
from random import randint
import logging
import asyncio

from mqtransport import MQApp, SQSApp
from mqtransport.participants import Producer
from settings import AppSettings, load_app_settings


class MP_ManyEven(Producer):

    name: str = "messages.rand.even"

    class Model(BaseModel):
        rnd: int


class MP_ManyOdd(Producer):

    name: str = "messages.rand.odd"

    class Model(BaseModel):
        rnd: int


class MQAppState:
    mp_even: MP_ManyEven
    mp_odd: MP_ManyOdd


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
        channel = await self._app.create_producing_channel(queues.many)

        mp_even = MP_ManyEven()
        channel.add_producer(mp_even)
        state.mp_even = mp_even

        mp_odd = MP_ManyOdd()
        channel.add_producer(mp_odd)
        state.mp_odd = mp_odd


async def create_mq_instance():
    settings = load_app_settings()
    initializer = MQAppProduceInitializer(settings)
    await initializer.do_init()
    return initializer.app


async def producing_loop_even(mq_app: MQApp):

    state: MQAppState = mq_app.state
    mp_even = state.mp_even

    while True:

        rand_num = randint(0, 100)
        if rand_num % 2 != 0:
            rand_num += 1

        await mp_even.produce(rnd=rand_num)
        mp_even.logger.info("[EVEN] Producing: rnd=%d", rand_num)
        await asyncio.sleep(2)


async def producing_loop_odd(mq_app: MQApp):

    state: MQAppState = mq_app.state
    mp_odd = state.mp_odd

    while True:

        rand_num = randint(0, 100)
        if rand_num % 2 == 0:
            rand_num += 1

        await mp_odd.produce(rnd=rand_num)
        mp_odd.logger.info("[ODD] Producing: rnd=%d", rand_num)
        await asyncio.sleep(2)


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

        tasks = [
            producing_loop_even(mq_app),
            producing_loop_odd(mq_app),
        ]

        logging.info("Running MQApp. Press Ctrl+C to exit")
        loop.run_until_complete(mq_app.start())
        loop.run_until_complete(asyncio.gather(*tasks))

    except KeyboardInterrupt as e:
        logging.warning("KeyboardInterrupt received")

    finally:
        logging.info("Shutting MQApp down")
        loop.run_until_complete(mq_app.shutdown())
