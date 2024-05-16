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

from google.cloud import run_v2
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

def get_run_execution_status(name: str):
    # Fetch data about a Google Cloud Run job execution
    # https://cloud.google.com/python/docs/reference/run/latest/google.cloud.run_v2.types.Execution
    client = run_v2.ExecutionsClient()

    request = run_v2.GetExecutionRequest(name=name)
    status = client.get_execution(request)
    complete_count = sum([
        status.succeeded_count,
        status.failed_count,
        status.cancelled_count,
    ])

    return {
        "complete_count": complete_count,
        "complete_percent": complete_count / status.task_count * 100,
        "running_count": status.running_count,
        "succeeded": status.succeeded_count,
        "failed": status.failed_count,
        "cancelled": status.cancelled_count,
        "retried": status.retried_count,
    }


def get_standard_quantity(quantity_string, standard_unit):
    # Using re.compile() + re.match() + re.groups()
    # Splitting text and number in string
    # temp = re.compile("([a-zA-Z]+)([0-9]+)")
    # res = temp.match(test_str).groups()
    ureg = UnitRegistry()

    # Handle unit variants
    ureg.define("Mi = 1 * MiB")
    ureg.define("millicore = 1 * m")

    qty = ureg(quantity_string)   
            
    return qty.to(standard_unit).magnitude


@asset
def job_quickstart(context: OpExecutionContext) -> MaterializeResult:
    """Execute a GCP Cloud Run Job"""

    job_name = "job-quickstart"
    location = "us-central1"
    project = "astute-fort-412223"
    job_timeout = 15 * 60

    request = run_v2.RunJobRequest(
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
            "task_count" : 10
        }
    )
    
    operation = run_v2.JobsClient().run_job(request)
    operation_metadata = MessageToDict(operation.metadata._pb)
    operation_name = operation_metadata.get("name")
    execution_id = operation_name.split("/")[-1]
    task_count = operation_metadata.get("taskCount")
    task_parallelism = operation_metadata.get("parallelism")
    task_container = operation_metadata.get("template").get("containers")[0]
    resources_per_task = task_container.get("resources").get("limits")
    
    # show full config for the operation
    context.log.debug(f"Operation Metadata:\n{operation_metadata}")
    
    # Monitor job execution
    context.log.info(f"Starting execution of '{operation_name}'...")
    context.log.info(f"View detailed task execution logs at:\n{operation_metadata.get('logUri')}")

    while operation.running():
        status = get_run_execution_status(operation_name)
        context.log.info(f"{round(status.get('complete_percent', 0))}% tasks completed ({status.get('complete_count', 0)}/{task_count}); {status.get('running_count', 0)} running")
        time.sleep(10)

    response = MessageToDict(operation.result(timeout=job_timeout)._pb)
    
    # show full results for the completed execution
    context.log.debug(f"Response:\n{response}")

    run_metadata = {
        "task_count": task_count,
        "parallelism": task_parallelism,
        "retry_percent": response.get("retriedCount") / task_count * 100,
        "task_cpu_millicore": get_standard_quantity(resources_per_task.get("cpu"), "millicore"),
        "task_memory_GiB": get_standard_quantity(resources_per_task.get("memory"), "GiB"),
        "details": UrlMetadataValue(f"https://console.cloud.google.com/run/jobs/executions/details/{location}/{execution_id}/tasks?project={project}"),
    }

    if response.get("retriedCount") > 0:
        raise Failure(
            description="Retries > 0",
            metadata=run_metadata,
        )

    return MaterializeResult(metadata=run_metadata)
    