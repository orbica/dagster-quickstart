from dagster import Definitions, load_assets_from_modules
from dagster_gcp.gcs import GCSPickleIOManager, GCSResource

from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
      "gcp_project": assets.GoogleCloudProjectResource(
        name="astute-fort-412223",
        location="us-central1",
      ),
        # "io_manager": GCSPickleIOManager(
        #     gcs_bucket="dagster-test-astute-fort-412223",
        #     # gcs_prefix="my-cool-prefix"
        # ),
        # "gcs": GCSResource(project="astute-fort-412223")
    }
)
