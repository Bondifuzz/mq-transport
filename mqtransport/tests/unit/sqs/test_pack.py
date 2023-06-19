import pytest
from pydantic import BaseModel

from mqtransport.src.sqs.pack import (
    pack_message,
    unpack_message,
    MessagePackError,
    MessageUnpackError,
    Compression,
)


def test_pack_unpack_ok():

    """
    Description:
        Pack message, then unpack it

    Succeeds:
        If packed message and unpacked message are the same
    """

    name = "pack"
    max_size = 100
    body = {"x": 1, "s": "str"}

    message = pack_message(name, body, max_size)
    compression = message["MessageAttributes"]["Compression"]
    assert compression["StringValue"] == Compression.off

    name_unpacked, body_unpacked = unpack_message(message)
    assert name == name_unpacked
    assert body == body_unpacked


def test_pack_unpack_pydantic_model_ok():

    """
    Description:
        Pack message, then unpack it.
        Message contains pydantic BaseModel

    Succeeds:
        If packed message and unpacked message are the same
    """

    class MyModel(BaseModel):
        x: int
        y: float
        s: str

    name = "pack"
    max_size = 100
    body = MyModel(x=1, y=2.0, s="3")

    message = pack_message(name, body, max_size)
    compression = message["MessageAttributes"]["Compression"]
    assert compression["StringValue"] == Compression.off

    name_unpacked, body_unpacked = unpack_message(message)
    assert MyModel.parse_obj(body_unpacked)
    assert name == name_unpacked


def test_pack_unpack_with_compression_ok():

    """
    Description:
        Pack message, then unpack it.
        Message is large, so compression must be used

    Succeeds:
        If packed message and unpacked message are the same
    """

    name = "pack"
    max_size = 10 ** 4
    body = {"x": 1, "s": "A" * max_size}

    message = pack_message(name, body, max_size)
    compression = message["MessageAttributes"]["Compression"]
    assert compression["StringValue"] == Compression.on

    name_unpacked, body_unpacked = unpack_message(message)
    assert name == name_unpacked
    assert body == body_unpacked


def test_pack_failed_name_empty():

    """
    Description:
        Try to pack message with empty name

    Succeeds:
        If exception raised
    """

    with pytest.raises(MessagePackError):
        pack_message("", {"a": "b"}, 100)


def test_pack_failed_body_not_serializable():

    """
    Description:
        Try to pack message with unserializable body

    Succeeds:
        If exception raised
    """

    with pytest.raises(MessagePackError):
        pack_message("a", {"a": bytes()}, 100)


def test_pack_failed_body_too_large():

    """
    Description:
        Try to pack message which is too large

    Succeeds:
        If exception raised
    """

    name = "pack"
    max_size = 10
    body = {"x": 1, "s": "A" * max_size}

    with pytest.raises(MessagePackError):
        pack_message(name, body, max_size)


def test_unpack_failed_invalid_name_attr():

    name = "unpack"
    max_size = 100
    body = {"x": 1, "s": "str"}

    message = pack_message(name, body, max_size)
    name = message["MessageAttributes"]["Name"]
    name["StringValue"] = ""

    with pytest.raises(MessageUnpackError):
        unpack_message(message)


def test_unpack_failed_invalid_compression_attr():

    name = "unpack"
    max_size = 100
    body = {"x": 1, "s": "str"}

    message = pack_message(name, body, max_size)
    compression = message["MessageAttributes"]["Compression"]
    compression["StringValue"] = "lalala"

    with pytest.raises(MessageUnpackError):
        unpack_message(message)


def test_unpack_failed_body_decompression_failed():

    name = "unpack"
    max_size = 100
    body = {"x": 1, "s": "str"}

    message = pack_message(name, body, max_size)
    compression = message["MessageAttributes"]["Compression"]
    assert compression["StringValue"] == Compression.off

    compression["StringValue"] = Compression.on
    with pytest.raises(MessageUnpackError):
        unpack_message(message)


def test_unpack_failed_body_not_deserializable():

    name = "unpack"
    max_size = 10 ** 4
    body = {"x": 1, "s": "A" * max_size}

    message = pack_message(name, body, max_size)
    compression = message["MessageAttributes"]["Compression"]
    assert compression["StringValue"] == Compression.on

    compression["StringValue"] = Compression.off
    with pytest.raises(MessageUnpackError):
        unpack_message(message)


def test_unpack_failed_body_not_dict():

    name = "unpack"
    max_size = 100
    body = {"x": 1, "s": "str"}

    message = pack_message(name, body, max_size)
    compression = message["MessageAttributes"]["Compression"]
    assert compression["StringValue"] == Compression.off

    message["MessageBody"] = ""
    with pytest.raises(MessageUnpackError):
        unpack_message(message)
