import asyncio
import pytest

from ..main.participants import MC_Normal, MP_Normal
from ..instance import MQAppState
from ..settings import AppSettings

from ..util import (
    SLEEP_TIME_SHORT,
    SLEEP_TIME_LONG,
    unordered_unique_match,
)

from .conftest import create_buggy_mq_instance
from .buggy_app import BuggyMQApp


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 2, 5])
async def test_lost_connection(buggy_mq_app: BuggyMQApp, n: int):

    """
    Description:
        Produce messages, which will not be received by
        message broker due to connection error. Then restore
        connection and ensure messages delivered successfully

    Succeeds:
        If messages delivered successfully when connection restored
    """

    producer = MP_Normal()
    consumer = MC_Normal()

    state: MQAppState = buggy_mq_app.state
    state.channels.och1.add_producer(producer)
    state.channels.ich1.add_consumer(consumer)
    await buggy_mq_app.start()

    # Make a connection error
    with buggy_mq_app.lose_connection():

        # Send N messages
        for _ in range(n):
            await producer.produce(x=1, y=1, cmd="add")

        # Give a try without connection
        await asyncio.sleep(SLEEP_TIME_LONG)

        # Nothing could be sent
        assert len(consumer.messages) == 0

    # Give a try with restored connection
    await asyncio.sleep(SLEEP_TIME_LONG + 0.5)
    await buggy_mq_app.stop()

    # When connection is restored,
    # messages must be delivered
    assert len(consumer.messages) == n
    assert unordered_unique_match(producer.messages, consumer.messages)


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 2, 5])
async def test_send_messages_before_shutdown(buggy_mq_app: BuggyMQApp, n: int):

    """
    Description:
        Checks mechanism which tries to deliver
        all unsent messages before app shutdown

    Succeeds:
        If unsent messages delivered
        successfully before app shutdown
    """

    producer = MP_Normal()
    consumer = MC_Normal()

    state: MQAppState = buggy_mq_app.state
    state.channels.och1.add_producer(producer)
    state.channels.ich1.add_consumer(consumer)

    # Make a connection error
    await buggy_mq_app.start()
    with buggy_mq_app.lose_connection():

        # Send N messages
        for _ in range(n):
            await producer.produce(x=1, y=1, cmd="add")

        # Give a try without connection
        await asyncio.sleep(SLEEP_TIME_LONG)

        # Nothing could be sent
        assert len(consumer.messages) == 0

    # Give a try with restored connection
    await buggy_mq_app.stop(timeout=SLEEP_TIME_LONG + 0.5)

    # Messages must be sent to message broker before application stop
    unsent_messages = buggy_mq_app.export_unsent_messages()
    assert len(unsent_messages[state.channels.och1.name]) == 0

    # Start app second time
    # to read messages sent previously
    await buggy_mq_app.start()
    await asyncio.sleep(SLEEP_TIME_SHORT)
    await buggy_mq_app.stop()

    # Ensure messages delivered
    assert len(consumer.messages) == n
    assert unordered_unique_match(producer.messages, consumer.messages)


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 2, 5])
async def test_import_export_unsent_messages(settings: AppSettings, n: int):

    """
    Description:
        Checks importing and exporting unsent messages

    Succeeds:
        If no errors occurred
    """

    producer1 = MP_Normal()
    producer2 = MP_Normal()
    consumer1 = MC_Normal()
    consumer2 = MC_Normal()
    buggy_mq_app1: BuggyMQApp = await create_buggy_mq_instance(settings)
    buggy_mq_app2: BuggyMQApp = await create_buggy_mq_instance(settings)

    state1: MQAppState = buggy_mq_app1.state
    state1.channels.och1.add_producer(producer1)
    state1.channels.ich1.add_consumer(consumer1)

    state2: MQAppState = buggy_mq_app2.state
    state2.channels.och1.add_producer(producer2)
    state2.channels.ich1.add_consumer(consumer2)

    try:
        # Make a connection error
        await buggy_mq_app1.start()
        with buggy_mq_app1.lose_connection():

            # Send N messages
            for _ in range(n):
                await producer1.produce(x=1, y=1, cmd="add")

            # No chance to send messages
            await buggy_mq_app1.stop(timeout=0)

        # Unsent messages can be exported
        unsent_messages = buggy_mq_app1.export_unsent_messages()
        assert len(unsent_messages[state1.channels.och1.name]) > 0

        # Unsent messages can be imported
        buggy_mq_app2.import_unsent_messages(unsent_messages)

        # Start second app and send imported messages
        # Also, it will receive these messages
        await buggy_mq_app2.start()
        await asyncio.sleep(SLEEP_TIME_SHORT)
        await buggy_mq_app2.stop()

        # Ensure messages delivered
        assert len(consumer2.messages) == n
        assert unordered_unique_match(producer1.messages, consumer2.messages)

        # First consumer didn't receive anything
        # Second producer didn't send anything
        assert len(producer2.messages) == 0
        assert len(consumer1.messages) == 0

    finally:
        await buggy_mq_app1.shutdown()
        await buggy_mq_app2.shutdown()
