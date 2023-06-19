import pytest

from mqtransport.errors import ChannelError
from mqtransport import MQApp

from ..util import mq_is_auth_disabled
from ..instance import create_mq_instance_with_auth_override


# @pytest.mark.skip()
@pytest.mark.asyncio
async def test_init_err_connection_failure():

    """
    Description:
        Emits connection failure

    Succeeds:
        If `ChannelError` error was raised
    """

    with pytest.raises(ChannelError):
        await create_mq_instance_with_auth_override(endpoint_url="http://no-such-url")


@pytest.mark.skipif(
    mq_is_auth_disabled(), reason="No authentication used in message broker"
)
@pytest.mark.asyncio
async def test_init_err_auth_failure(settings):

    """
    Description:
        Emits authentication failures

    Succeeds:
        If `ChannelError` error was raised
    """

    with pytest.raises(ChannelError):
        await create_mq_instance_with_auth_override(username="no-such-username")

    with pytest.raises(ChannelError):
        await create_mq_instance_with_auth_override(password="no-such-password")


@pytest.mark.asyncio
async def test_init_err_no_queue_found(mq_app_empty: MQApp):

    """
    Description:
        Tries to use non-existent channel

    Succeeds:
        If `ChannelError` error was raised
    """

    with pytest.raises(ChannelError):
        await mq_app_empty.create_consuming_channel("no-such-channel")

    with pytest.raises(ChannelError):
        await mq_app_empty.create_producing_channel("no-such-channel")
