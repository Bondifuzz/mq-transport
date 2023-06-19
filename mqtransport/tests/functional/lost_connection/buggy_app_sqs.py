from contextlib import contextmanager
from typing import Callable, Optional

from mqtransport import SQSApp
from botocore.exceptions import ClientError
from .buggy_app import BuggyMQApp


class BuggySQSApp(SQSApp, BuggyMQApp):

    _send_message_backup: Callable
    _receive_message_backup: Callable

    async def _init(
        self,
        endpoint_url: str,
        region_name: str,
        username: str,
        password: str,
    ):
        await super()._init(endpoint_url, region_name, username, password)
        self._receive_message_backup = self._client.receive_message
        self._send_message_backup = self._client.send_message

    @staticmethod
    async def create(
        username: str,
        password: str,
        region_name: str,
        endpoint_url: Optional[str] = None,
    ):
        _self = BuggySQSApp()
        await _self._init(
            username,
            password,
            region_name,
            endpoint_url,
        )
        return _self

    async def _hook_send_message(
        self,
        QueueUrl,
        MessageBody,
        DelaySeconds=None,
        MessageAttributes=None,
        MessageSystemAttributes=None,
        MessageDeduplicationId=None,
        MessageGroupId=None,
    ) -> dict:

        error = {
            "Error": {
                "Code": 1,
                "Message": "Connection failed (hooked)",
            }
        }

        raise ClientError(error, "send_message")

    async def _hook_receive_message(
        self,
        QueueUrl,
        AttributeNames=None,
        MessageAttributeNames=None,
        MaxNumberOfMessages=None,
        VisibilityTimeout=None,
        WaitTimeSeconds=None,
        ReceiveRequestAttemptId=None,
    ) -> dict:

        error = {
            "Error": {
                "Code": 1,
                "Message": "Connection failed (hooked)",
            }
        }

        raise ClientError(error, "receive_message")

    def _make_connection_ok(self):
        self._client.send_message = self._send_message_backup
        self._client.receive_message = self._receive_message_backup

    def _make_connection_lost(self):
        self._client.send_message = self._hook_send_message
        self._client.receive_message = self._hook_receive_message

    @contextmanager
    def lose_connection(self):
        try:
            self._make_connection_lost()
            yield
        finally:
            self._make_connection_ok()
