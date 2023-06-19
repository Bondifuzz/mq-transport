from multiprocessing.pool import ApplyResult
import multiprocessing as mp
from random import randint
from typing import List
import asyncio
import pytest

from mqtransport import MQApp

from ..util import unordered_unique_match, SLEEP_TIME_LONG
from ..instance import create_mq_instance_empty
from ..participants import MP_Base, MC_Base
from ..settings import AppSettings


class MP_ThroughputLow(MP_Base):

    name: str = "throughput-low"

    class Model(MP_Base.Model):
        rnd: int


class MC_ThroughputLow(MC_Base):

    delay_sec = 0.05
    name: str = "throughput-low"

    class Model(MC_Base.Model):
        rnd: int

    async def consume(self, msg: Model, app: MQApp):
        await asyncio.sleep(self.delay_sec)
        return await super().consume(msg, app)


async def consuming_task(settings: AppSettings, run_time_sec: float):

    mq_app = await create_mq_instance_empty(settings)
    consumer = MC_ThroughputLow()

    queues = settings.message_queue.queues
    ich1 = await mq_app.create_consuming_channel(queues.test1)
    ich1.add_consumer(consumer)

    await mq_app.start()
    await asyncio.sleep(run_time_sec)
    await mq_app.shutdown()

    return consumer.messages


def consume_messages(settings: AppSettings, run_time_sec: float, res_queue: mp.Queue):
    messages = asyncio.run(consuming_task(settings, run_time_sec))
    res_queue.put(messages)


# @pytest.mark.skip()
@pytest.mark.asyncio
@pytest.mark.parametrize("n_workers", [2, 3, 5, 10])
@pytest.mark.parametrize("n_messages", [1, 20, 100])
async def test_consume_concurrently(
    mq_app_empty: MQApp, settings: AppSettings, n_workers: int, n_messages: int
):
    """
    Description:
        Checks ability to consume messages concurrently in process pool.
        First, write messages to queue, but do not consume them.
        Then create pool of consumer processes
        and start consuming messages.

    Succeeds:
        If no errors were encountered, produced messages match consumed messages
    """

    #
    # Prepare clear channel and producer
    #

    producer = MP_ThroughputLow()
    queues = settings.message_queue.queues
    och1 = await mq_app_empty.create_producing_channel(queues.test1)
    och1.add_producer(producer)
    await och1.purge()

    #
    # Write N messages to channel
    # These messages will be consumed later
    #

    async def produce_task(producer: MP_ThroughputLow):
        await producer.produce(rnd=randint(-(10 ** 6), 10 ** 6))

    tasks = []
    for _ in range(n_messages):
        tasks.append(produce_task(producer))

    await mq_app_empty.start()
    await asyncio.gather(*tasks)
    await mq_app_empty.stop()

    #
    # Create pool of consumer processes
    # They will consume messages in parallel
    # Consumed messages will be written to `mp.Queue`
    #

    manager = mp.Manager()
    res_queue = manager.Queue()

    with mp.Pool(n_workers) as worker_pool:

        delay_sec = MC_ThroughputLow.delay_sec * 3
        run_time_sec = SLEEP_TIME_LONG + (delay_sec * n_messages / n_workers)
        futures: List[ApplyResult] = []

        # Distribute consuming tasks over processes in pool
        # Use `apply_async` to ensure they're working concurrently

        for _ in range(n_workers):
            futures.append(
                worker_pool.apply_async(
                    func=consume_messages,
                    args=(settings, run_time_sec, res_queue),
                )
            )

        for fut in futures:
            fut.get()

    consumed_messages = []
    while not res_queue.empty():
        messages: list = res_queue.get()
        consumed_messages.extend(messages)

    #
    # Ensure produced and consumed messages match
    #

    assert unordered_unique_match(
        producer.messages, consumed_messages
    ), "Produced and consumed messages do not match"
