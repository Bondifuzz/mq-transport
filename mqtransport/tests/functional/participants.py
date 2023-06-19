from __future__ import annotations
from typing import TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel
from ...participants import Producer, Consumer

if TYPE_CHECKING:
    from ... import MQApp


class UUIDMessage(BaseModel):

    uid: str = ""

    def __hash__(self):
        return hash(frozenset(self.dict().items()))

    def __eq__(self, other):
        assert isinstance(other, UUIDMessage)
        return self.dict() == other.dict()


class MC_Base(Consumer):

    _messages: list

    class Model(UUIDMessage):
        pass

    def __init__(self):
        super().__init__()
        self._messages = []

    async def consume(self, msg: Model, app: MQApp):
        self._messages.append(msg)

    @property
    def messages(self):
        return self._messages


class MP_Base(Producer):

    _messages: list

    class Model(UUIDMessage):
        pass

    def __init__(self):
        super().__init__()
        self._messages = []

    async def produce(self, **kwargs):
        body = {**kwargs, "uid": uuid4().hex}
        self._messages.append(self.Model(**body))
        await super().produce(**body)

    @property
    def messages(self):
        return self._messages
