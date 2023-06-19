from __future__ import annotations
from functools import reduce
from typing import Dict, Optional
from contextlib import suppress
from pydantic import BaseModel
from random import randint
import logging
import asyncio
import json
import os

from mqtransport import MQApp, SQSApp
from mqtransport.participants import Producer
from settings import AppSettings, load_app_settings


class MP_Unsent(Producer):

    name: str = "messages.rand"

    class Model(BaseModel):
        rnd: int


class MQAppState:
    mp_unsent: MP_Unsent


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
        channel = await self._app.create_producing_channel(queues.unsent)

        mp_unsent = MP_Unsent()
        channel.add_producer(mp_unsent)
        state.mp_unsent = mp_unsent


async def create_mq_instance():
    settings = load_app_settings()
    initializer = MQAppProduceInitializer(settings)
    await initializer.do_init()
    return initializer.app


async def producing_loop(mq_app: MQApp, no_kill_lock: asyncio.Lock):

    with suppress(asyncio.CancelledError):

        state: MQAppState = mq_app.state
        mp_failure = state.mp_unsent

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


def dump_unsent_messages(unsent_messages: Dict[str, list]):

    filepath = "unsent_messages.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(unsent_messages, f)


def load_unsent_messages() -> Optional[Dict[str, list]]:

    filepath = "unsent_messages.json"
    if not os.path.isfile(filepath):
        logging.info("Unsent messages loaded: 0")
        return None

    with open(filepath, "r", encoding="utf-8") as f:
        unsent_messages: dict = json.load(f)

    return unsent_messages


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
    messages = load_unsent_messages()

    if messages:
        mq_app.import_unsent_messages(messages)

    try:
        logging.info("Running MQApp. Press Ctrl+C to exit")
        loop.run_until_complete(mq_app.start())
        task = loop.create_task(producing_loop(mq_app, lock))
        loop.run_until_complete(asyncio.wait_for(task, timeout=None))

    except KeyboardInterrupt as e:
        logging.warning("KeyboardInterrupt received")

    finally:
        logging.info("Shutting MQApp down in 5 seconds...")
        loop.run_until_complete(stop_producing_loop(task, lock))
        loop.run_until_complete(mq_app.shutdown(timeout=5))
        dump_unsent_messages(mq_app.export_unsent_messages())
