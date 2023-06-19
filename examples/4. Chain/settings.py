from contextlib import suppress
from async_timeout import Optional
from pydantic import BaseSettings, BaseModel, Field, AnyUrl

# fmt: off
with suppress(ModuleNotFoundError):
    import dotenv; dotenv.load_dotenv()
# fmt: on


class MessageQueues(BaseSettings):
    one_two: str = "mqtransport-examples-chain-one-two"
    two_three: str = "mqtransport-examples-chain-two-three"


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
