import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from .clickhouse import DATABASE, get_client

CSV_PATH = Path(os.getenv("EARTHQUAKE_CSV_PATH", "/opt/dagster/data/katalog_gempa.csv"))

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "earthquake_dbt").resolve()
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()

RAW_DDL = f"""
CREATE TABLE IF NOT EXISTS {DATABASE}.raw_earthquakes (
    tgl String, ot String, lat String, lon String, depth String, mag String,
    remark String,
    strike1 String, dip1 String, rake1 String,
    strike2 String, dip2 String, rake2 String,
    _source_file String,
    _ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY tuple()
"""

DQ_DDL = f"""
CREATE TABLE IF NOT EXISTS {DATABASE}.meta_dq_results (
    executed_at DateTime,
    dagster_run_id String,
    node_name String,
    resource_type String,
    status String,
    message String,
    execution_time_s Float64
) ENGINE = MergeTree ORDER BY (executed_at, node_name)
"""


@asset(compute_kind="python", group_name="bronze", key="raw_earthquakes")
def raw_earthquakes(context: AssetExecutionContext) -> MaterializeResult:
    """Loads the BMKG CSV into the ClickHouse Bronze layer exactly as it is (all columns as String)."""
    # keep_default_na=False + dtype=str: no parsing, no NaN coercion -> raw fidelity.
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False)
    df["_source_file"] = CSV_PATH.name

    client = get_client()
    client.command(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    client.command(RAW_DDL)
    # Full reload: truncate first so re-materialising never duplicates rows.
    client.command(f"TRUNCATE TABLE {DATABASE}.raw_earthquakes")
    client.insert_df(f"{DATABASE}.raw_earthquakes", df)

    return MaterializeResult(
        metadata={
            "row_count": len(df),
            "source_file": str(CSV_PATH),
            "preview": MetadataValue.md(df.head().to_markdown(index=False)),
        }
    )


@asset_check(asset="raw_earthquakes", description="Bronze row count equals CSV row count")
def raw_earthquakes_row_count() -> AssetCheckResult:
    expected = len(pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False))
    actual = get_client().command(f"SELECT count() FROM {DATABASE}.raw_earthquakes")
    return AssetCheckResult(
        passed=int(actual) == expected,
        metadata={"expected": expected, "actual": int(actual)},
    )


class EarthquakeDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        # Map the dbt source straight onto the Dagster Bronze asset.
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        fqn = dbt_resource_props.get("fqn", [])
        return fqn[1] if len(fqn) > 2 else None


def _record_dq_results(context: AssetExecutionContext, results: list[dict]) -> None:
    """Persist dbt model/test outcomes so analysts can query data-quality status in ClickHouse."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        [
            now,
            context.run_id,
            r["unique_id"].split(".", 2)[-1],
            r["unique_id"].split(".", 1)[0],
            r["status"],
            (r.get("message") or "")[:500],
            float(r.get("execution_time") or 0),
        ]
        for r in results
    ]
    client = get_client()
    client.command(DQ_DDL)
    client.insert(
        f"{DATABASE}.meta_dq_results",
        rows,
        column_names=[
            "executed_at", "dagster_run_id", "node_name", "resource_type",
            "status", "message", "execution_time_s",
        ],
    )


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=EarthquakeDbtTranslator())
def earthquake_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Runs `dbt build` (models + tests) to produce the Silver and Gold layers."""
    invocation = dbt.cli(["build"], context=context)
    try:
        yield from invocation.stream()
    finally:
        # Record results even when a test fails, so failures are visible to analysts.
        try:
            run_results = invocation.get_artifact("run_results.json")
            _record_dq_results(context, run_results["results"])
        except Exception as exc:  # never mask the real dbt failure
            context.log.warning(f"Could not record dbt results: {exc}")
