from __future__ import annotations
from typing import Union
import functools

from botocore.exceptions import ClientError, HTTPClientError
from aiohttp.client_exceptions import ClientConnectionError

from ..base.channel import ConsumingChannel, ProducingChannel
from ..base.errors import ChannelError


def wrap_botocore_errors(func):

    CCH = ConsumingChannel
    PCH = ProducingChannel
    Channel = Union[PCH, CCH]

    @functools.wraps(func)
    async def wrapper(_self: Channel, *args, **kwargs):

        assert isinstance(_self, PCH) or isinstance(_self, CCH)
        qname = _self.name

        try:
            res = await func(_self, *args, **kwargs)

        except ClientConnectionError as e:
            raise ChannelError("Message broker connection failure") from e

        except ClientError as e:

            error_code = e.response["Error"]["Code"]

            if error_code == "AccessDenied":
                msg = f"Not enough rights to work with message queue '{qname}'"
                raise ChannelError(msg) from e

            if error_code == "InvalidAccessKeyId":
                msg = "Message broker authentication failed - Invalid access key"
                raise ChannelError(msg) from e

            if error_code == "SignatureDoesNotMatch":
                msg = "Message broker authentication failed - Invalid secret key"
                raise ChannelError(msg) from e

            if error_code == "AWS.SimpleQueueService.NonExistentQueue":
                raise ChannelError(f"Message queue '{qname}' does not exist") from e

            raise ChannelError(f"Error in channel '{qname}' - {str(e)}") from e

        except HTTPClientError as e:
            raise ChannelError(f"Error in channel '{qname}' - {str(e)}") from e

        return res

    return wrapper
