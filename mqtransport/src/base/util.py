import os
import functools
import asyncio


def testing_only(func):

    """Provides decorator, which forbids
    calling dangerous functions in production"""

    env = os.getenv("MQ_TRANSPORT_TESTING", "")
    is_forbidden = not (env.lower() == "true" or env == "1")

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):

        if is_forbidden:
            raise RuntimeError(
                "Dangerous calls are forbidden. Use MQ_TRANSPORT_TESTING=1 to enable them"
            )

        await func(*args, **kwargs)

    return wrapper


async def delay(sec=5):
    """ Sleeps `sec` seconds """
    await asyncio.sleep(sec)
