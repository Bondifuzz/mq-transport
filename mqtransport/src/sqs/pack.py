from base64 import b85encode, b85decode
from gzip import compress, decompress
from typing import Tuple

from pydantic import BaseModel


def default(obj):
    if isinstance(obj, BaseModel):
        return obj.dict()
    raise TypeError


try:
    # fmt: off
    import orjson # type: ignore
    json_dumps = lambda x: orjson.dumps(x, default).decode()
    json_loads = lambda x: orjson.loads(x)
    # fmt: on

except ModuleNotFoundError:
    import json  # fmt: skip
    json_dumps = lambda x: json.dumps(x, default=default)
    json_loads = lambda x: json.loads(x)


class MessagePackError(Exception):
    pass


class MessageUnpackError(Exception):
    pass


class Compression:
    on = "on"
    off = "off"


def pack_message(name: str, body: dict, max_size: int) -> dict:

    if len(name) == 0:
        raise MessagePackError(f"Message name is empty")

    try:
        jbody = json_dumps(body)
    except (ValueError, TypeError) as e:
        raise MessagePackError(f"Failed to pack message body - {e}")

    if len(jbody) > max_size:
        compression = Compression.on
        jbody = b85encode(compress(jbody.encode())).decode()
    else:
        compression = Compression.off

    if len(jbody) > max_size:
        raise MessagePackError("Message body too large")

    return {
        "MessageAttributes": {
            "Name": {
                "StringValue": name,
                "DataType": "String",
            },
            "Compression": {
                "StringValue": compression,
                "DataType": "String",
            },
        },
        "MessageBody": jbody,
    }


def unpack_message(msg: dict) -> Tuple[str, dict]:

    # Fit both `boto3.receive_messages` and `pack_message`
    body = msg.get("Body") or msg.get("MessageBody")
    attrs = msg.get("MessageAttributes")

    if body is None:
        raise MessageUnpackError("Message does not have a body")

    if attrs is None:
        raise MessageUnpackError("Message does not have attributes")

    try:
        assert isinstance(attrs, dict)
        name = attrs["Name"]["StringValue"]
        compression = attrs["Compression"]["StringValue"]

    except KeyError as e:
        err = f"Message attribute missing: {e}"
        raise MessageUnpackError(err, msg) from e

    if len(name) == 0:
        raise MessageUnpackError(f"Message name is empty")

    if compression not in [Compression.off, Compression.on]:
        err = f"Invalid Compression attribute: {compression}"
        raise MessageUnpackError(err, msg)

    if compression == Compression.on:
        try:
            body = decompress(b85decode(body)).decode()
        except Exception as e:
            err = f"Failed to decompress message body - {e}"
            raise MessageUnpackError(err, msg) from e

    try:
        body = json_loads(body)
    except (ValueError, TypeError) as e:
        raise MessageUnpackError(f"Failed to parse message body - {e}", msg)

    if not isinstance(body, dict):
        raise MessageUnpackError(f"Message body is not a dictionary", msg)

    return name, body
