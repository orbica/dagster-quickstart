import time
from typing import Any, Dict, Optional
from pydantic import Field

from dagster import (
    ConfigurableResource,
    Failure,
    OpExecutionContext,
    ResourceDependency,
    UrlMetadataValue,
)

from google.cloud.run_v2 import ExecutionsClient, JobsClient, RunJobRequest
from google.protobuf.json_format import MessageToDict

from dagster_quickstart.util import get_standard_quantity

class GoogleCloudProjectResource(ConfigurableResource):
    name: str = Field(
        description="unique ID of the GCP project, e.g. 'my-project-123456'",
    )

    location: str = Field(
        description="GCP region to deploy resources to, e.g. 'us-central1'",
    )

class CloudRunJobResource(ConfigurableResource):
    name: str # The name must use only lowercase alphanumeric characters and dashes, cannot begin or end with a dash, and cannot be longer than 63 characters.
    project: ResourceDependency[GoogleCloudProjectResource]

    status_poll_seconds: Optional[int] = Field(
        description="time in seconds to poll the Cloud Run API for updates",
        default=10,
    )

    output_path: Optional[str] = Field(
        description="relative file path from DATA_PATH, to store task outputs",
        default=None,
    )

    def execute(self, context: OpExecutionContext, task_count: int = None, timeout_seconds: int = None, env: Dict[str, Any] = {}) -> RunJobRequest:
        full_name = f"projects/{self.project.name}/locations/{self.project.location}/jobs/{self.name}"
        context.log.debug(f"CloudRunJobResource: {full_name}")

        if self.output_path != None:
            env.update({"OUTPUT_PATH": self.output_path})

        # https://cloud.google.com/python/docs/reference/run/latest/google.cloud.run_v2.types.RunJobRequest.Overrides
        overrides = {
            "container_overrides":[{
                "env": [ {"name": key, "value": str(value) } for key, value in env.items() ]
            }],   
        }

        if task_count != None:
            overrides.update({"task_count" : task_count})

        if timeout_seconds != None:
            overrides.update({"timeout": f"{timeout_seconds}s"})

        context.log.debug(f"CloudRunJobResource: overrides:\n{overrides}")

        request = RunJobRequest(name=full_name, overrides=overrides)

        operation = JobsClient().run_job(request)

        # execution metadata
        operation_metadata = MessageToDict(operation.metadata._pb)
        context.log.debug(f"Operation Metadata:\n{operation_metadata}")

        operation_name = operation_metadata.get("name")
        execution_id = operation_name.split("/")[-1]
        task_container = operation_metadata.get("template").get("containers")[0]
        resources_per_task = task_container.get("resources").get("limits")
        
        context.add_output_metadata({
            "task_count": operation_metadata.get("taskCount"),
            "task_parallelism": operation_metadata.get("parallelism"),
            "task_cpu_millicore": get_standard_quantity(resources_per_task.get("cpu"), "millicore"),
            "task_memory_GiB": get_standard_quantity(resources_per_task.get("memory"), "GiB"),
            "details": UrlMetadataValue(f"https://console.cloud.google.com/run/jobs/executions/details/{self.project.location}/{execution_id}/tasks?project={self.project.name}"),
            "output_path": self.output_path,
        })
        
        # Monitor job execution
        context.log.info(f"Starting execution of '{operation_name}'...")
        context.log.info(f"View detailed task execution logs at:\n{operation_metadata.get('logUri')}")

        # Fetch data about a Google Cloud Run job execution
        # https://cloud.google.com/python/docs/reference/run/latest/google.cloud.run_v2.types.Execution
        executions_client = ExecutionsClient()
        while operation.running():
            time.sleep(self.status_poll_seconds)
            try:
                status = executions_client.get_execution(name=operation_name)
            
                complete_count = sum([
                    status.succeeded_count,
                    status.failed_count,
                    status.cancelled_count,
                ])
                complete_percent = complete_count / status.task_count * 100
                retry_percent = status.retried_count / status.task_count * 100

                context.add_output_metadata({
                    "retry_percent": retry_percent,
                })
                
                context.log.info(f"{round(complete_percent)}% tasks completed ({complete_count}/{status.task_count}); {status.running_count} running")
            
            except Exception as error:
                context.log.warn(f"unable to get execution '{operation_name}':\n{error}\n\nSkipping status check...")

        # handle completion
        response = None
        try:
            response = operation.result(timeout=timeout_seconds)
        except Exception:
            raise Failure(description="Cloud Run Job Failure")
        finally:
            context.log.debug(f"Response:\n{response}") 
            # response_dict = MessageToDict(response._pb)