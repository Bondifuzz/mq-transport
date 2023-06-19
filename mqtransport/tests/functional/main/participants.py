from __future__ import annotations
from typing import TYPE_CHECKING, List

from mqtransport import MQApp
from mqtransport.tests.functional.settings import load_app_settings

from ..participants import MP_Base, MC_Base
from ..instance import MQAppState

########################################
# Normal
########################################


class ModelNormal(MP_Base.Model):
    cmd: str
    x: int
    y: int


class MP_Normal(MP_Base):

    name: str = "normal"

    class Model(ModelNormal):
        cmd: str
        x: int
        y: int


class MC_Normal(MC_Base):

    name: str = "normal"

    class Model(ModelNormal):
        cmd: str
        x: int
        y: int


########################################
# Throughput
########################################


class ModelThroughput(MP_Base.Model):
    rnd: int


class MP_Throughput(MP_Base):

    name: str = "throughput"

    class Model(ModelThroughput):
        pass


class MP_Throughput1(MP_Base):

    name: str = "throughput-1"

    class Model(ModelThroughput):
        pass


class MP_Throughput2(MP_Base):

    name: str = "throughput-2"

    class Model(ModelThroughput):
        pass


class MP_Throughput3(MP_Base):

    name: str = "throughput-3"

    class Model(ModelThroughput):
        pass


class MC_Throughput(MC_Base):

    name: str = "throughput"

    class Model(ModelThroughput):
        pass


class MC_Throughput1(MC_Throughput):

    name: str = "throughput-1"

    class Model(ModelThroughput):
        pass


class MC_Throughput2(MC_Throughput):

    name: str = "throughput-2"

    class Model(ModelThroughput):
        pass


class MC_Throughput3(MC_Throughput):

    name: str = "throughput-3"

    class Model(ModelThroughput):
        pass


########################################
# Reacting
########################################


class ModelReacting(MP_Base.Model):
    pass


class MP_Reacting(MP_Base):

    name: str = "reacting"

    class Model(ModelReacting):
        pass


class MC_Reacting(MC_Base):

    name: str = "reacting"

    class Model(ModelReacting):
        pass

    async def consume(self, msg: Model, app: MQApp):
        await super().consume(msg, app)
        state: MQAppState = app.state
        mp_normal = state.producers.normal
        await mp_normal.produce(cmd="add", x=1, y=2)


########################################
# Complex type
########################################


class ModelComplexType(MP_Base.Model):
    list_of_str: List[str]


class MP_ComplexType(MP_Base):

    name: str = "complex-type"

    class Model(ModelComplexType):
        pass


class MC_ComplexType(MC_Base):

    name: str = "complex-type"

    class Model(ModelComplexType):
        pass


########################################
# Bad format
########################################


class ModelBadFormat(MP_Base.Model):
    num: int


class MP_BadFormat(MP_Base):

    name: str = "bad-format"

    class Model(ModelBadFormat):
        pass

    async def produce_bad_msg_no_such_name(self):
        body = self.Model(num=1).dict()
        await self.channel.send_message("no-such-name", body)

    async def produce_bad_msg_unpack_failure(self):

        settings = load_app_settings()
        broker = settings.message_queue.broker

        if broker == "sqs":
            from mqtransport.src.sqs.pack import pack_message
            from mqtransport.src.sqs.producing import SQSProducingChannel

            message = pack_message(self.name, {}, 2)
            message["MessageBody"] = "123"

            channel: SQSProducingChannel = self.channel
            await channel.send_message_raw(message)
            return

        raise ValueError(f"Invalid message broker: {broker}")

    async def produce_bad_msg_invalid_model(self):
        body = self.Model(num=1).dict()
        body["num"] = "num-is-invalid-must-be-int"
        await self.channel.send_message(self.name, body)


class MC_BadFormat(MC_Base):

    name: str = "bad-format"

    class Model(ModelBadFormat):
        pass


########################################
# Unhandled error
########################################


class MC_UnhandledError(MC_Base):

    name: str = "unhandled-error"

    class Model(ModelThroughput):
        pass

    async def consume(self, *unused):
        raise Exception("Unhandled")


class MP_UnhandledError(MP_Base):

    name: str = "unhandled-error"

    class Model(ModelThroughput):
        pass
