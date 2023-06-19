import pytest
import asyncio
from mqtransport import MQApp

from ..instance import create_mq_instance, MQAppState, create_mq_instance_empty
from ..settings import AppSettings

from pydantic import BaseModel
from mqtransport.participants import Producer, Consumer


class MP_Stub(Producer):

    name = "myproducer"

    class Model(BaseModel):
        pass


class MC_Stub(Consumer):

    name = "myconsumer"

    class Model(BaseModel):
        pass

    async def consume(self, msg: Model, app: MQApp):
        pass


########################################
# Usage Errors
########################################


@pytest.mark.asyncio
async def test_basic_usage(settings):
    """
    Description:
        Basic usage: connect and not forget to call `shutdown` at exit

    Succeeds:
        If no errors were encountered
    """

    mq_app = await create_mq_instance(settings)

    try:
        await mq_app.ping()
    finally:
        await mq_app.shutdown()


def test_register_producer_twice(mq_app: MQApp):
    """
    Description:
        Usage error: attempt to add the same producer to channel twice

    Succeeds:
        If assertion error was raised
    """

    state: MQAppState = mq_app.state
    och1 = state.channels.och1
    och1.add_producer(MP_Stub())

    with pytest.raises(AssertionError):
        och1.add_producer(MP_Stub())


def test_register_consumer_twice(mq_app: MQApp):
    """
    Description:
        Usage error: attempt to add the same consumer to channel twice

    Succeeds:
        If assertion error was raised
    """

    state: MQAppState = mq_app.state
    ich1 = state.channels.ich1
    ich1.add_consumer(MC_Stub())

    with pytest.raises(AssertionError):
        ich1.add_consumer(MC_Stub())


@pytest.mark.asyncio
async def test_use_consuming_channel_twice(mq_app_empty: MQApp, settings: AppSettings):
    """
    Description:
        Usage error: attempt to use the same channel twice

    Succeeds:
        If assertion error was raised
    """

    queues = settings.message_queue.queues
    await mq_app_empty.create_consuming_channel(queues.test1)
    with pytest.raises(AssertionError):
        await mq_app_empty.create_consuming_channel(queues.test1)


@pytest.mark.asyncio
async def test_use_producing_channel_twice(mq_app_empty: MQApp, settings: AppSettings):
    """
    Description:
        Usage error: attempt to use the same channel twice

    Succeeds:
        If assertion error was raised
    """

    queues = settings.message_queue.queues
    await mq_app_empty.create_producing_channel(queues.test1)
    with pytest.raises(AssertionError):
        await mq_app_empty.create_producing_channel(queues.test1)


@pytest.mark.asyncio
async def test_start_no_consumers(mq_app_empty: MQApp, settings: AppSettings):
    """
    Description:
        Usage error: call `start` without binding
        at least one producer or consumer to channel

    Succeeds:
        If assertion error was raised
    """

    queues = settings.message_queue.queues
    ich1 = await mq_app_empty.create_consuming_channel(queues.test1)
    och1 = await mq_app_empty.create_producing_channel(queues.test1)
    och1.add_producer(MP_Stub())

    with pytest.raises(AssertionError):
        await mq_app_empty.start()


@pytest.mark.asyncio
async def test_start_no_producers(mq_app_empty: MQApp, settings: AppSettings):
    """
    Description:
        Usage error: call `start` without binding
        at least one producer or consumer to channel

    Succeeds:
        If assertion error was raised
    """

    queues = settings.message_queue.queues
    ich1 = await mq_app_empty.create_consuming_channel(queues.test1)
    och1 = await mq_app_empty.create_producing_channel(queues.test1)
    ich1.add_consumer(MC_Stub())

    # Start is possible, but warning will be printed
    await mq_app_empty.start()


@pytest.mark.asyncio
async def test_start_no_channels(mq_app_empty: MQApp):
    """
    Description:
        Usage error: call `start` without using
        at least one producing or consuming channel

    Succeeds:
        If assertion error was raised
    """

    with pytest.raises(AssertionError):
        await mq_app_empty.start()


@pytest.mark.asyncio
async def test_mq_app_start_twice(mq_app: MQApp):
    """
    Description:
        Usage error: call `start` twice

    Succeeds:
        If assertion error was raised
    """

    state: MQAppState = mq_app.state
    state.channels.ich1.add_consumer(MC_Stub())
    state.channels.och1.add_producer(MP_Stub())

    await mq_app.start()
    with pytest.raises(AssertionError):
        await mq_app.start()


@pytest.mark.asyncio
async def test_stop_before_start(mq_app: MQApp):
    """
    Description:
        Usage error: call `stop` before `start`

    Succeeds:
        If assertion error was raised
    """

    with pytest.raises(AssertionError):
        await mq_app.stop()


@pytest.mark.asyncio
async def test_stop_twice(mq_app: MQApp):
    """
    Description:
        Usage error: call `stop` twice

    Succeeds:
        If assertion error was raised
    """

    state: MQAppState = mq_app.state
    state.channels.ich1.add_consumer(MC_Stub())
    state.channels.och1.add_producer(MP_Stub())

    await mq_app.start()
    await asyncio.sleep(1)

    await mq_app.stop()
    with pytest.raises(AssertionError):
        await mq_app.stop()


@pytest.mark.asyncio
async def test_shutdown_before_start(settings):
    """
    Description:
        Usage error: call `shutdown` before app start

    Succeeds:
        If assertion error was raised
    """

    mq_app = await create_mq_instance_empty(settings)
    await mq_app.shutdown()


@pytest.mark.asyncio
async def test_shutdown_twice(settings):
    """
    Description:
        Usage error: call `shutdown` twice

    Succeeds:
        If assertion error was raised
    """

    mq_app = await create_mq_instance_empty(settings)
    await mq_app.shutdown()

    with pytest.raises(AssertionError):
        await mq_app.shutdown()


@pytest.mark.asyncio
async def test_run_forever_without_consuming(mq_app_empty: MQApp):
    """
    Description:
        Checks that `run_forever` can be called only
        if at least one consuming channel is being used.
        *If we're not waiting for incoming messages,
        why should we get into infinite loop?*

    Succeeds:
        If assertion error is raised
    """
    with pytest.raises(AssertionError):
        await mq_app_empty.run_forever()
