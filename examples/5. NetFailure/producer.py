from __future__ import annotations
from contextlib import suppress
from pydantic import BaseModel
from random import randint
import logging
import asyncio

from mqtransport import MQApp, SQSApp
from mqtransport.participants import Producer
from settings import AppSettings, load_app_settings


class MP_NetFailure(Producer):

    name: str = "messages.rand"

    class Model(BaseModel):
        rnd: int


class MQAppState:
    mp_failure: MP_NetFailure


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
        channel = await self._app.create_producing_channel(queues.net_failure)

        mp_failure = MP_NetFailure()
        channel.add_producer(mp_failure)
        state.mp_failure = mp_failure


async def create_mq_instance():
    settings = load_app_settings()
    initializer = MQAppProduceInitializer(settings)
    await initializer.do_init()
    return initializer.app


async def producing_loop(mq_app: MQApp, no_kill_lock: asyncio.Lock):

    with suppress(asyncio.CancelledError):

        state: MQAppState = mq_app.state
        mp_failure = state.mp_failure

        while True:

            async with no_kill_lock:
                rand_num = randint(0, 100)
                await mp_failure.produce(rnd=rand_num)
                mp_failure.logger.info("Producing: rnd=%d", rand_num)

            await asyncio.sleep(1)


async def stop_producing_loop(
    task: asyncio.Task,
    no_kill_lock: asyncio.Lock,
):
    async with no_kill_lock:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


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

    lock = asyncio.Lock()
    loop = asyncio.get_event_loop()

    logging.info("Creating MQApp")
    mq_app = loop.run_until_complete(create_mq_instance())

    try:
        logging.info("Running MQApp. Press Ctrl+C to exit")
        loop.run_until_complete(mq_app.start())
        task = loop.create_task(producing_loop(mq_app, lock))
        loop.run_until_complete(asyncio.wait_for(task, timeout=None))

    except KeyboardInterrupt as e:
        logging.warning("KeyboardInterrupt received")

    finally:
        logging.info("Shutting MQApp down")
        loop.run_until_complete(stop_producing_loop(task, lock))
        loop.run_until_complete(mq_app.shutdown())
