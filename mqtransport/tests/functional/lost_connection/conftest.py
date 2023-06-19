import pytest_asyncio

from mqtransport import MQApp
from ..instance import MQAppInitializer
from .buggy_app_sqs import BuggySQSApp


class BuggyMQAppInitializer(MQAppInitializer):

    """Creates intialized instance of buggy MQ app"""

    async def _create_mq_app(self):

        mq_broker = self._settings.message_queue.broker.lower()
        mq_settings = self._settings.message_queue

        if mq_broker == "sqs":
            app = await BuggySQSApp.create(
                mq_settings.username,
                mq_settings.password,
                mq_settings.region,
                mq_settings.url,
            )
        else:
            raise ValueError(f"Unsupported message broker: {mq_broker}")

        return app


async def create_buggy_mq_instance(settings):
    initializer = BuggyMQAppInitializer(settings)
    await initializer.do_init()
    return initializer.app


@pytest_asyncio.fixture()
async def buggy_mq_app(settings) -> MQApp:
    mq_app = await create_buggy_mq_instance(settings)
    yield mq_app
    await mq_app.shutdown()
