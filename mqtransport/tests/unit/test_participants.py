import pytest
from pydantic import BaseModel

from mqtransport.src.base.app import MQApp
from mqtransport.src.base.participants import Consumer, Producer


def test_abstract():

    """
    Description:
        Try to instantiate objects of base classes.
        Instantiation must fail, because base objects
        don't have names and message models defined

    Succeeds:
        If AssertionError is raised
    """

    with pytest.raises(AssertionError):
        Producer()

    with pytest.raises(AssertionError):
        Consumer()


class MyProducer(Producer):

    name = "myproducer"

    class Model(BaseModel):
        x: int
        s: str


class MyConsumer(Consumer):

    name = "myconsumer"

    class Model(BaseModel):
        x: int
        s: str

    async def consume(self, msg: Model, app: MQApp):
        pass


def test_concrete_producer():

    """
    Description:
        Define concrete producer class
        with name and message model defined.

    Succeeds:
        If no errors occurred
    """

    producer = MyProducer()
    assert producer.name == "myproducer"

    res = producer.Model.parse_obj({"x": 1, "s": "str"})
    assert res.s == "str"
    assert res.x == 1


def test_concrete_consumer():

    """
    Description:
        Define concrete consumer class
        with name and message model defined.

    Succeeds:
        If no errors occurred
    """

    producer = MyConsumer()
    assert producer.name == "myconsumer"

    res = producer.Model.parse_obj({"x": 1, "s": "str"})
    assert res.s == "str"
    assert res.x == 1
