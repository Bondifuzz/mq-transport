import pytest_asyncio
import pytest

from .settings import load_app_settings, AppSettings
from .instance import create_mq_instance, create_mq_instance_empty
from mqtransport import MQApp


@pytest.fixture(scope="session")
def settings() -> AppSettings:
    return load_app_settings()


@pytest_asyncio.fixture()
async def mq_app(settings) -> MQApp:
    mq_app = await create_mq_instance(settings)
    yield mq_app
    await mq_app.shutdown()


@pytest_asyncio.fixture()
async def mq_app_empty(settings) -> MQApp:
    mq_app = await create_mq_instance_empty(settings)
    yield mq_app
    await mq_app.shutdown()
