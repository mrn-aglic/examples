from typing import Any, Optional

from airflow.models.expandinput import MappedArgument
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from dags.httpasync.httptrigger import HttpTrigger


class HttpSensorAsync(BaseSensorOperator):
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        http_conn_id: str,
        endpoint: str = "",
        method: str = "GET",
        data: Optional[dict | list | MappedArgument] = None,
        headers: Optional[dict] = None,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        **kwargs: Any
    ):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.method = method
        self.data = data or {}
        self.headers = headers or {}
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    def execute(self, context: Context) -> Any:
        if isinstance(self.data, MappedArgument):
            data = self.data.resolve(context)
        else:
            data = self.data

        print(data)
        print(type(data))

        self.defer(
            trigger=HttpTrigger(
                http_conn_id=self.http_conn_id,
                method=self.method,
                endpoint=self.endpoint,
                data=data,
                headers=self.headers,
                retry_limit=self.retry_limit,
                retry_delay=self.retry_delay,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Optional[dict] = None) -> None:
        self.log.info(event)
        self.log.info("%s completed successfully.", self.task_id)
        self.log.info(event["num-results"])
        self.log.info(event["data"])
        return event
