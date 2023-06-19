from __future__ import annotations
from pydantic import BaseModel
import logging
import asyncio

from mqtransport import MQApp, SQSApp
from mqtransport.participants import Consumer
from settings import AppSettings, load_app_settings


class MC_NetFailure(Consumer):

    name: str = "messages.rand"

    class Model(BaseModel):
        rnd: int

    async def consume(self, msg: Model, mq_app: MQApp):
        self._logger.info("Consumed message: rnd=%d", msg.rnd)


class MQAppInitializer:

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
        self._app.state = None

        try:
            await self._app.ping()
            await self._configure_input_channels()

        except:
            await self._app.shutdown()
            raise

    async def _create_mq_app(self):

        broker = self._settings.message_queue.broker.lower()
        settings = self._settings.message_queue

        if broker == "sqs":
            return await SQSApp.create(
                settings.username,
                settings.password,
                settings.region,
                settings.url,
            )

        raise ValueError(f"Unsupported message broker: {broker}")

    async def _configure_input_channels(self):
        queues = self._settings.message_queue.queues
        channel = await self._app.create_consuming_channel(queues.net_failure)
        channel.add_consumer(MC_NetFailure())


async def create_mq_instance():
    settings = load_app_settings()
    initializer = MQAppInitializer(settings)
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
