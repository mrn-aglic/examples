import asyncio
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from feat22.mypkg.hookasync import HttpAsyncHook


class HttpTrigger(BaseTrigger):
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        endpoint: str,
        http_conn_id: str,
        method: str = "GET",
        data: Optional[dict] = None,
        headers: Optional[dict] = None,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ):
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.method = method
        self.data = data or {}
        self.headers = headers or {}
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        http_hook = self._get_async_hook()

        retry_num = 1

        while retry_num <= self.retry_limit:
            try:
                await http_hook.run(
                    endpoint=self.endpoint, data=self.data, headers=self.headers
                )
                yield TriggerEvent(True)
            except AirflowException as e:
                await asyncio.sleep(self.retry_delay)

                retry_num = retry_num + 1

                if retry_num > self.retry_limit:
                    raise e

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "feat22.mypkg.httptrigger.HttpTrigger",
            {
                "endpoint": self.endpoint,
                "http_conn_id": self.http_conn_id,
                "method": self.method,
                "headers": self.headers,
                "retry_limit": self.retry_limit,
                "retry_delay": self.retry_delay,
                "data": self.data,
            },
        )

    def _get_async_hook(self):
        return HttpAsyncHook(self.http_conn_id, method=self.method)
