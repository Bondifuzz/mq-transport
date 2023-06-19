from __future__ import annotations
from typing import TYPE_CHECKING, Callable
from random import randint
import asyncio
import pytest

from pydantic import ValidationError

from ..util import (
    unordered_unique_match,
    SLEEP_TIME_SHORT,
    SLEEP_TIME_LONG,
)

from .participants import (
    MP_Normal,
    MC_Normal,
    MP_Reacting,
    MC_Reacting,
    MP_Throughput,
    MC_Throughput,
    MP_Throughput1,
    MC_Throughput1,
    MP_Throughput2,
    MC_Throughput2,
    MP_Throughput3,
    MC_Throughput3,
    MP_ComplexType,
    MC_ComplexType,
    MP_BadFormat,
    MC_BadFormat,
    MP_UnhandledError,
    MC_UnhandledError,
)


if TYPE_CHECKING:
    from ..instance import MQAppState, MQApp


########################################
# Basic usage
########################################


async def produce_task(producer: MP_Throughput):
    await producer.produce(rnd=randint(-(10 ** 6), 10 ** 6))


@pytest.mark.asyncio
@pytest.mark.parametrize("run_time_sec", [0, 1, 2])
async def test_mq_app_start_stop(mq_app: MQApp, run_time_sec: int):
    """
    Description:
        Create mq_app, do "start" and "stop" operations in several time ranges

    Succeeds:
        If no errors were encountered
    """

    state: MQAppState = mq_app.state
    state.channels.ich1.add_consumer(MC_Normal())
    state.channels.och1.add_producer(MP_Normal())

    await mq_app.start()
    await asyncio.sleep(run_time_sec)
    await mq_app.stop()


@pytest.mark.asyncio
async def test_one_producer_one_consumer(mq_app: MQApp):
    """
    Description:
        Create mq_app and start one producer and one consumer.
        Producer produces message, consumer consumes message.

    Succeeds:
        If no errors were encountered, produced messages match consumed messages
    """

    producer = MP_Normal()
    consumer = MC_Normal()

    state: MQAppState = mq_app.state
    state.channels.och1.add_producer(producer)
    state.channels.ich1.add_consumer(consumer)

    await mq_app.start()
    await producer.produce(cmd="add", x=1, y=2)
    await asyncio.sleep(SLEEP_TIME_SHORT)
    await mq_app.stop()

    assert len(producer.messages) == 1, "Message not produced"
    assert len(consumer.messages) == 1, "Message not consumed"

    assert unordered_unique_match(
        producer.messages, consumer.messages
    ), "Produced and consumed messages do not match"


@pytest.mark.asyncio
async def test_two_producers_two_consumers(mq_app: MQApp):
    """
    Description:
        Create mq_app and start two producers and two consumers.
        Producer1 produces message, consumer1 consumes message.
        Consumer1 calls producer2 to produce message,
        which will be handled by consumer2.

    Succeeds:
        If no errors were encountered, produced messages match consumed messages
    """
    producer1 = MP_Reacting()
    consumer1 = MC_Reacting()
    producer2 = MP_Normal()
    consumer2 = MC_Normal()

    state: MQAppState = mq_app.state
    state.channels.och1.add_producer(producer1)
    state.channels.och1.add_producer(producer2)
    state.channels.ich1.add_consumer(consumer1)
    state.channels.ich1.add_consumer(consumer2)
    state.producers.normal = producer2

    await mq_app.start()
    await producer1.produce()
    await asyncio.sleep(SLEEP_TIME_SHORT)
    await mq_app.stop()

    assert unordered_unique_match(
        producer1.messages, consumer1.messages
    ), "Produced and consumed messages (1) do not match"

    assert unordered_unique_match(
        producer2.messages, consumer2.messages
    ), "Produced and consumed messages (2) do not match"


@pytest.mark.asyncio
@pytest.mark.parametrize("n_messages", [1, 2, 3, 5, 10, 20, 50])
async def test_channel_throughput(mq_app: MQApp, n_messages: int):
    """
    Description:
        Create mq_app, start 3 producers and 3 consumers.
        All producers and consumers are simultaneously communicating.

    Succeeds:
        If no errors were encountered, produced messages match consumed messages
    """
    producer1 = MP_Throughput1()
    producer2 = MP_Throughput2()
    producer3 = MP_Throughput3()
    consumer1 = MC_Throughput1()
    consumer2 = MC_Throughput2()
    consumer3 = MC_Throughput3()

    state: MQAppState = mq_app.state
    state.channels.och1.add_producer(producer1)
    state.channels.och1.add_producer(producer2)
    state.channels.och1.add_producer(producer3)
    state.channels.ich1.add_consumer(consumer1)
    state.channels.ich1.add_consumer(consumer2)
    state.channels.ich1.add_consumer(consumer3)

    tasks = []
    for _ in range(n_messages):
        tasks.append(produce_task(producer1))
        tasks.append(produce_task(producer2))
        tasks.append(produce_task(producer3))

    await mq_app.start()
    await asyncio.gather(*tasks)
    if n_messages >= 10:
        await asyncio.sleep(SLEEP_TIME_LONG)
    else:
        await asyncio.sleep(SLEEP_TIME_SHORT)
    await mq_app.stop()

    assert unordered_unique_match(
        producer1.messages, consumer1.messages
    ), "Produced and consumed messages (1) do not match"

    assert unordered_unique_match(
        producer2.messages, consumer2.messages
    ), "Produced and consumed messages (2) do not match"

    assert unordered_unique_match(
        producer3.messages, consumer3.messages
    ), "Produced and consumed messages (3) do not match"


########################################
# Produce bad Model
########################################


@pytest.mark.asyncio
async def test_produce_complex_model(mq_app: MQApp):
    """
    Description:
        Create mq_app, start producer and consumer.
        Producer uses complex type, which requires
        making call to pydantic's `Model.update_forward_refs()`.
        This test ensures that complex types are handled by default

    Succeeds:
        If no errors were encountered, produced messages match consumed messages
    """
    producer = MP_ComplexType()
    consumer = MC_ComplexType()

    state: MQAppState = mq_app.state
    state.channels.och1.add_producer(producer)
    state.channels.ich1.add_consumer(consumer)

    await mq_app.start()
    await producer.produce(list_of_str=["a", "b", "c"])
    await asyncio.sleep(SLEEP_TIME_SHORT)
    await mq_app.stop()

    # Can't use this check on complex types
    # assert unordered_unique_match(
    #     producer.messages, consumer.messages
    # ), "Produced and consumed messages do not match"

    assert len(producer.messages) == 1
    assert len(consumer.messages) == 1

    message_src = producer.messages[0]
    message_dst = consumer.messages[0]
    assert message_src == message_dst


@pytest.mark.asyncio
async def test_produce_bad_model_invalid_attr(mq_app: MQApp):
    """
    Description:
        Try to send message not matching the specified Model (Invalid attribute)

    Succeeds:
        If pydantic's `ValidationError` is raised and message was not sent
    """

    producer = MP_Normal()
    consumer = MC_Normal()

    state: MQAppState = mq_app.state
    state.channels.och1.add_producer(producer)
    state.channels.ich1.add_consumer(consumer)

    await mq_app.start()
    with pytest.raises(ValidationError):
        await producer.produce(x="a", y=1, cmd="xor")
    await mq_app.stop()

    # No messages received
    assert not consumer.messages


@pytest.mark.asyncio
async def test_produce_bad_model_no_attr(mq_app: MQApp):
    """
    Description:
        Try to send message not matching the specified Model (No attribute)

    Succeeds:
        If pydantic's `ValidationError` is raised and message was not sent
    """

    producer = MP_Normal()
    consumer = MC_Normal()

    state: MQAppState = mq_app.state
    state.channels.och1.add_producer(producer)
    state.channels.ich1.add_consumer(consumer)

    await mq_app.start()
    with pytest.raises(ValidationError):
        await producer.produce(x=1, y=1)
    await mq_app.stop()

    # No messages received
    assert not consumer.messages


########################################
# Consume errors
########################################


async def consume_err_verify(mq_app: MQApp, bad_produce_func: Callable):

    producer = MP_BadFormat()
    consumer = MC_BadFormat()

    state: MQAppState = mq_app.state
    state.channels.och1.add_producer(producer)
    state.channels.ich1.add_consumer(consumer)

    ####################
    # Round 1: good

    await mq_app.start()
    await producer.produce(num=1)
    await asyncio.sleep(SLEEP_TIME_SHORT)
    await mq_app.stop()

    # Message processed
    assert len(consumer.messages) == 1

    ####################
    # Round 2: bad

    await mq_app.start()
    await bad_produce_func(producer)
    await asyncio.sleep(SLEEP_TIME_SHORT)
    await mq_app.stop()

    # Message was not processed
    assert len(consumer.messages) == 1


@pytest.mark.asyncio
async def test_consume_err_no_consumer_found(mq_app: MQApp):
    """
    Description:
        Consumer received message with bad format: message name is not registered
        This message must be discarded.

    Succeeds:
        If message was never processed
    """

    async def produce_func(producer: MP_BadFormat):
        await producer.produce_bad_msg_no_such_name()

    await consume_err_verify(mq_app, bad_produce_func=produce_func)


@pytest.mark.asyncio
async def test_consume_err_message_unpack_failure(mq_app: MQApp):
    """
    Description:
        Consumer received message with bad format: message unpack failure
        This message must be discarded.

    Succeeds:
        If `ConsumeMessageError` is raised and message was not processed
    """

    async def produce_func(producer: MP_BadFormat):
        await producer.produce_bad_msg_unpack_failure()

    await consume_err_verify(mq_app, bad_produce_func=produce_func)


@pytest.mark.asyncio
async def test_consume_err_invalid_model(mq_app: MQApp):
    """
    Description:
        Consumer received message with bad format: message model is invalid.
        This message must be discarded.

    Succeeds:
        If `ConsumeMessageError` is raised and message was not processed
    """

    async def produce_func(producer: MP_BadFormat):
        await producer.produce_bad_msg_invalid_model()

    await consume_err_verify(mq_app, bad_produce_func=produce_func)


@pytest.mark.asyncio
@pytest.mark.parametrize("n_messages", [1, 2, 3, 5, 10])
async def test_consume_err_unhandled_exception(mq_app: MQApp, n_messages: int):
    """
    Description:
        Consumer received message and started processing it.
        But an unhandled exception has occurred.
        That must not affect consuming channel.

    Succeeds:
        If unhandled exception always gets catched
        and other consumers are able to work despite errors
    """

    producer1 = MP_Throughput1()
    consumer1 = MC_Throughput1()
    producer2 = MP_UnhandledError()
    consumer2 = MC_UnhandledError()

    state: MQAppState = mq_app.state
    state.channels.och1.add_producer(producer1)
    state.channels.och1.add_producer(producer2)
    state.channels.ich1.add_consumer(consumer1)
    state.channels.ich1.add_consumer(consumer2)

    tasks = []
    for _ in range(n_messages):
        tasks.append(produce_task(producer1))
        tasks.append(produce_task(producer2))

    await mq_app.start()
    await asyncio.gather(*tasks)
    await asyncio.sleep(SLEEP_TIME_SHORT)
    await mq_app.stop()

    assert len(producer2.messages) == n_messages
    assert len(consumer2.messages) == 0

    assert unordered_unique_match(
        producer1.messages, consumer1.messages
    ), "Produced and consumed messages (1) do not match"


########################################
# Run forever
########################################


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [0, 1, 3])
async def test_run_forever(mq_app: MQApp, timeout: int):
    """
    Description:
        Checks that `run_forever` mechanism works.
        `MQApp` gets into infinite wait loop when `run_forever` is called,
        so just create task, wait some time and kill it.

    Succeeds:
        If no errors were encountered
    """

    task = asyncio.create_task(mq_app.run_forever())
    await asyncio.sleep(timeout)
    task.cancel()


@pytest.mark.asyncio
@pytest.mark.parametrize("n_messages", [1, 2, 3, 5, 10])
async def test_run_forever_throughput(mq_app: MQApp, n_messages: int):
    """
    Description:
        The same as `test_run_forever`, but with
        message producing and consuming.

    Succeeds:
        If no errors were encountered, produced messages match consumed messages
    """

    producer1 = MP_Throughput1()
    producer2 = MP_Throughput2()
    consumer1 = MC_Throughput1()
    consumer2 = MC_Throughput2()

    state: MQAppState = mq_app.state
    state.channels.och1.add_producer(producer1)
    state.channels.och1.add_producer(producer2)
    state.channels.ich1.add_consumer(consumer1)
    state.channels.ich1.add_consumer(consumer2)

    tasks = []
    for _ in range(n_messages):
        tasks.append(produce_task(producer1))
        tasks.append(produce_task(producer2))

    task = asyncio.create_task(mq_app.run_forever())
    await asyncio.gather(*tasks)
    await asyncio.sleep(SLEEP_TIME_SHORT)
    task.cancel()

    assert unordered_unique_match(
        producer1.messages, consumer1.messages
    ), "Produced and consumed messages (1) do not match"

    assert unordered_unique_match(
        producer2.messages, consumer2.messages
    ), "Produced and consumed messages (2) do not match"
