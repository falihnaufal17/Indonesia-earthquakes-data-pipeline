from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource

from .assets import (
    dbt_project,
    earthquake_dbt_assets,
    raw_earthquakes,
    raw_earthquakes_row_count,
)

earthquake_pipeline_job = define_asset_job(
    "earthquake_pipeline_job",
    selection=AssetSelection.all(),
    description="Bronze ingest -> dbt Silver/Gold build + tests",
)

daily_schedule = ScheduleDefinition(
    job=earthquake_pipeline_job,
    cron_schedule="0 2 * * *",
)

defs = Definitions(
    assets=[raw_earthquakes, earthquake_dbt_assets],
    asset_checks=[raw_earthquakes_row_count],
    jobs=[earthquake_pipeline_job],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
