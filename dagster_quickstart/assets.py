from typing import Any, Dict

import pandas as pd

from dagster import (
    AutoMaterializePolicy,
    OpExecutionContext,
    asset,
)

from dagster_quickstart.resources import CloudRunJobResource, GoogleCloudProjectResource

@asset
def upstream():
    # simulate reading config (e.g. data file paths) from remote system and pass reference to outputs downstream
    return {
        "count": 5,
        "output_path": "."
    }   
    
@asset
def multiply_2(context: OpExecutionContext, gcp_project: GoogleCloudProjectResource, upstream) -> Dict[str, Any]:
    """Multiply task index by 2 in a GCP Cloud Run Job"""
    
    task_count=upstream.get("count", None)

    job = CloudRunJobResource(
        name = "multiply",
        project = gcp_project,
        output_path = f"{'/'.join(context.asset_key.path)}/{context.run_id}",
    )

    job.execute(
        context=context,
        task_count=task_count,
        env={
            "FAIL_RATE": 0,
            "MULTIPLY_BY": 2,
            "INPUT_UPSTREAM": upstream.get("output_path", "."),
        }
    )
    
    return  {
        "count": task_count,
        "output_path": job.output_path
    }

@asset(auto_materialize_policy=AutoMaterializePolicy.eager())
def multiply_4(context: OpExecutionContext, gcp_project: GoogleCloudProjectResource, multiply_2) -> Dict[str, Any]:
    """Multiply input value by 4 in a Cloud Run Job"""
    
    task_count=multiply_2.get("count", None)

    job = CloudRunJobResource(
        name = "multiply",
        project = gcp_project,
        output_path = f"{'/'.join(context.asset_key.path)}/{context.run_id}",
    )

    job.execute(
        context=context,
        task_count=task_count,
        env={
            "FAIL_RATE": 0,
            "MULTIPLY_BY": 4,
            "INPUT_UPSTREAM": multiply_2.get("output_path", "."),
        }
    )
    
    return  {
        "count": task_count,
        "output_path": job.output_path
    }