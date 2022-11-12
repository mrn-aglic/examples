import asyncio
from typing import Any, Optional

import aiohttp
from aiohttp import BasicAuth, ClientResponseError
from airflow import AirflowException
from airflow.hooks.base import BaseHook
from asgiref.sync import sync_to_async


class HttpAsyncHook(BaseHook):
    def __init__(
        self,
        conn_id: str,
        method: str = "GET",
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ):
        super().__init__()

        self.conn_id = conn_id
        self.method = method.upper()
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

        if retry_limit < 1 or retry_delay < 1:
            raise ValueError(
                "Retry limit and retry delay need to be larger or equal to 1"
            )

    async def run(
        self,
        endpoint: str,
        headers: Optional[dict] = None,
        data: Optional[dict] = None,
    ):
        headers = headers or {}
        data = data or {}

        conn = await self.get_conn()

        base_url = f"{conn.conn_type}://{conn.host}"
        if conn.schema:
            base_url = f"{base_url}/{conn.schema}"

        if conn.port:
            base_url = f"{base_url}:{conn.port}"

        if conn.extra:
            extra = conn.extra_dejson
            headers.update(extra)

        auth = None
        if conn.login:
            auth = BasicAuth(conn.login, conn.password)

        url = f"{base_url}/{endpoint}"

        async with aiohttp.ClientSession() as session:
            if self.method == "GET":
                request_func = session.get
            elif self.method == "POST":
                request_func = session.post
            else:
                raise AirflowException("Unsupported http method")

            attempt_num = 1

            while attempt_num <= self.retry_limit:
                response = await request_func(
                    url,
                    json=data if self.method == "POST" else None,
                    params=data if self.method == "GET" else None,
                    auth=auth,
                )

                try:
                    response.raise_for_status()
                    return response

                except ClientResponseError as e:
                    self.log.warning(
                        "[Try %d of %d] Request to %s failed.",
                        attempt_num,
                        self.retry_limit,
                        url,
                    )
                    if attempt_num > self.retry_limit:
                        self.log.exception("HTTP error with status: %s", e.status)
                        raise AirflowException(str(e.status) + ":" + e.message) from e

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)

    def get_conn(self) -> Any:
        return sync_to_async(self.get_connection)(self.conn_id)
