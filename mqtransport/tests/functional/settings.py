from cmath import log
from typing import Optional
from contextlib import suppress
import logging

from pydantic import (
    BaseSettings,
    BaseModel,
    AnyUrl,
    Field,
)

# fmt: off
with suppress(ModuleNotFoundError):
    import dotenv; dotenv.load_dotenv()
# fmt: on


fmt = "%(asctime)s %(levelname)-8s %(name)-15s %(message)s"
logging.basicConfig(format=fmt, level=logging.DEBUG)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)


class MessageQueues(BaseSettings):
    test1: str
    dlq: str

    class Config:
        env_prefix = "MQ_QUEUE_"


class MessageQueueSettings(BaseSettings):

    url: Optional[AnyUrl]
    broker: str = Field(regex="^sqs$")
    queues: MessageQueues
    username: str
    password: str
    region: str

    class Config:
        env_prefix = "MQ_"


class AppSettings(BaseModel):
    message_queue: MessageQueueSettings


def load_app_settings():
    return AppSettings(
        message_queue=MessageQueueSettings(
            queues=MessageQueues(),
        ),
    )
