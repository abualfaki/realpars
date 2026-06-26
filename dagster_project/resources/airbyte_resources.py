"""Airbyte resource for syncing data from Circle.so to BigQuery."""

import logging
import time
from collections.abc import Mapping
from typing import Any

import requests
from dagster import Failure
from dagster_airbyte import AirbyteCloudResource
from dagster_airbyte.translator import AirbyteJobStatusType
from dagster_airbyte.types import AirbyteOutput
from requests import RequestException

from configs.config import (
    AIRBYTE_CLIENT_ID,
    AIRBYTE_CLIENT_SECRET,
    AIRBYTE_WORKSPACE_ID,
)

logger = logging.getLogger(__name__)


def _require_config_value(value: str | None, env_var: str) -> str:
    if not value:
        raise ValueError(f"Set {env_var} in your environment to use the Airbyte resource.")
    return value


class ResilientAirbyteCloudResource(AirbyteCloudResource):
    workspace_id: str | None = None

    def _list_jobs_for_connection(self, connection_id: str) -> list[Mapping[str, Any]]:
        headers = {
            "accept": "application/json",
            **self.all_additional_request_params.get("headers", {}),
        }
        params: dict[str, Any] = {"connectionId": connection_id, "limit": 25}
        if self.workspace_id:
            params["workspaceIds"] = self.workspace_id

        num_retries = 0
        while True:
            try:
                response = requests.get(
                    f"{self.api_base_url}/jobs",
                    headers=headers,
                    params=params,
                    timeout=max(self.request_timeout, 30),
                )
                response.raise_for_status()

                payload = response.json()
                return payload.get("data", [])
            except RequestException as error:
                self._log.warning(
                    "Unable to list Airbyte jobs for connection_id=%s: %s",
                    connection_id,
                    error,
                )
                if num_retries == self.request_max_retries:
                    return []
                num_retries += 1
                time.sleep(self.request_retry_delay)

    def _get_existing_running_job(self, connection_id: str) -> Mapping[str, object] | None:
        jobs = self._list_jobs_for_connection(connection_id)
        running_jobs = [
            job
            for job in jobs
            if job.get("status")
            in {
                AirbyteJobStatusType.RUNNING,
                AirbyteJobStatusType.PENDING,
                AirbyteJobStatusType.INCOMPLETE,
            }
        ]

        if not running_jobs:
            return None

        latest_job = max(running_jobs, key=lambda job: int(job.get("jobId", 0)))
        job_id = latest_job.get("jobId")
        if job_id is None:
            return None

        return {"job": {"id": int(job_id), "status": latest_job.get("status")}}

    def sync_and_poll(
        self,
        connection_id: str,
        poll_interval: float | None = None,
        poll_timeout: float | None = None,
    ):
        connection_details = self.get_connection_details(connection_id)
        job_details: Mapping[str, object] | None = None
        existing_job = self._get_existing_running_job(connection_id)

        if existing_job:
            job_details = existing_job
            job_info = dict(existing_job.get("job", {}))
            job_id = int(job_info["id"])
            self._log.info(
                "Job %s already running for connection_id=%s. Resume polling.",
                job_id,
                connection_id,
            )
        else:
            try:
                job_details = self.start_sync(connection_id)
            except Failure as error:
                if "409 Client Error: Conflict" not in str(error):
                    raise

                existing_job = self._get_existing_running_job(connection_id)
                if not existing_job:
                    raise

                job_details = existing_job
                job_info = dict(existing_job.get("job", {}))
                job_id = int(job_info["id"])
                self._log.info(
                    "Connection %s already has an active Airbyte job %s. Resume polling.",
                    connection_id,
                    job_id,
                )
            else:
                job_info = dict(job_details.get("job", {}))
                job_id = int(job_info["id"])
                self._log.info("Job %s initialized for connection_id=%s.", job_id, connection_id)

        start = time.monotonic()
        state = job_info.get("status")

        try:
            while True:
                if poll_timeout and start + poll_timeout < time.monotonic():
                    raise Failure(
                        f"Timeout: Airbyte job {job_id} is not ready after the timeout"
                        f" {poll_timeout} seconds"
                    )

                time.sleep(poll_interval or self.poll_interval)
                job_details = self.get_job_status(connection_id, job_id)
                job_info = dict(job_details.get("job", {}))
                state = job_info.get("status")

                if state in (
                    AirbyteJobStatusType.RUNNING,
                    AirbyteJobStatusType.PENDING,
                    AirbyteJobStatusType.INCOMPLETE,
                ):
                    continue
                if state == AirbyteJobStatusType.SUCCEEDED:
                    break
                if state == AirbyteJobStatusType.ERROR:
                    raise Failure(f"Job failed: {job_id}")
                if state == AirbyteJobStatusType.CANCELLED:
                    raise Failure(f"Job was cancelled: {job_id}")

                raise Failure(f"Encountered unexpected state `{state}` for job_id {job_id}")
        finally:
            if (
                state
                not in (
                    AirbyteJobStatusType.SUCCEEDED,
                    AirbyteJobStatusType.ERROR,
                    AirbyteJobStatusType.CANCELLED,
                )
                and self.cancel_sync_on_run_termination
            ):
                self.cancel_job(job_id)

        return AirbyteOutput(job_details=job_details, connection_details=connection_details)


airbyte_resource = ResilientAirbyteCloudResource(
    client_id=_require_config_value(AIRBYTE_CLIENT_ID, "AIRBYTE_CLIENT_ID"),
    client_secret=_require_config_value(AIRBYTE_CLIENT_SECRET, "AIRBYTE_CLIENT_SECRET"),
    workspace_id=AIRBYTE_WORKSPACE_ID,
)
