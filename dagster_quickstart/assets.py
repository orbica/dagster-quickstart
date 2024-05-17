import json
import requests

import pandas as pd

from dagster import (
    Failure,
    MaterializeResult,
    MetadataValue,
    OpExecutionContext,
    UrlMetadataValue,
    asset,
)
from dagster_quickstart.configurations import HNStoriesConfig

from google.cloud.run_v2 import ExecutionsClient, JobsClient, RunJobRequest
from google.protobuf.json_format import MessageToDict
from pint import UnitRegistry
import time

@asset
def hackernews_top_story_ids(config: HNStoriesConfig):
    """Get top stories from the HackerNews top stories endpoint."""
    top_story_ids = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json").json()

    with open(config.hn_top_story_ids_path, "w") as f:
        json.dump(top_story_ids[: config.top_stories_limit], f)


@asset(deps=[hackernews_top_story_ids])
def hackernews_top_stories(config: HNStoriesConfig) -> MaterializeResult:
    """Get items based on story ids from the HackerNews items endpoint."""
    with open(config.hn_top_story_ids_path, "r") as f:
        hackernews_top_story_ids = json.load(f)

    results = []
    for item_id in hackernews_top_story_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)

    df = pd.DataFrame(results)
    df.to_csv(config.hn_top_stories_path)

    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(str(df[["title", "by", "url"]].to_markdown())),
        }
    )


def get_standard_quantity(quantity_string, standard_unit):
    ureg = UnitRegistry()

    # Handle unit variants
    ureg.define("Mi = 1 * MiB")
    ureg.define("millicore = 1 * m")

    qty = ureg(quantity_string)   
            
    return qty.to(standard_unit).magnitude

@asset
def upstream():
    return [1, 2, 3]

@asset
def job_quickstart(context: OpExecutionContext, upstream) -> None:
    """Execute a GCP Cloud Run Job"""

    # configure job execution
    job_name = "job-quickstart"
    location = "us-central1"
    project = "astute-fort-412223"
    job_timeout_seconds = 15 * 60
    status_poll_seconds = 5

    context.log.info(f"input: {upstream}")

    request = RunJobRequest(
        name=f"projects/{project}/locations/{location}/jobs/{job_name}", 
        # https://cloud.google.com/python/docs/reference/run/latest/google.cloud.run_v2.types.RunJobRequest.Overrides
        overrides={
            "container_overrides":[{
                "env": [
                    {
                        "name": "FAIL_RATE", 
                        "value": "0.2"
                    }]
            }],
            "timeout": f"{str(5 * 60)}s",
            "task_count" : len(upstream)
        }
    )

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
        "details": UrlMetadataValue(f"https://console.cloud.google.com/run/jobs/executions/details/{location}/{execution_id}/tasks?project={project}"),
    })
    
    # Monitor job execution
    context.log.info(f"Starting execution of '{operation_name}'...")
    context.log.info(f"View detailed task execution logs at:\n{operation_metadata.get('logUri')}")

    # Fetch data about a Google Cloud Run job execution
    # https://cloud.google.com/python/docs/reference/run/latest/google.cloud.run_v2.types.Execution
    executions_client = ExecutionsClient()
    while operation.running():
        time.sleep(status_poll_seconds)
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
        response = operation.result(timeout=job_timeout_seconds)
    except Exception:
        raise Failure(description="Cloud Run Job Failure")
    finally:
        context.log.debug(f"Response:\n{response}") 
        # response_dict = MessageToDict(response._pb)
    