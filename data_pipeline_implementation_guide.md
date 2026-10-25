# Technical Implementation Guide: Earthquake Data Pipeline

## 1. Architecture Overview & Goals

### Pipeline Goal (Data Analyst Perspective)

From a Data Analyst's perspective, the primary goal of this pipeline is to transform raw, semi-structured seismic logs into a **reliable, self-service analytics environment**. By abstracting away the data engineering complexities, this architecture ensures analysts can focus entirely on deriving insights rather than cleaning data. Key analyst-driven objectives include:

* **A Single Source of Truth:** Providing standardized metrics (magnitude, depth) and dimensions (geography, time) to ensure all organizational reporting is consistent.
* **High-Performance Exploration:** Utilizing ClickHouse's columnar engine to allow analysts to filter and aggregate massive datasets instantly without query timeouts.
* **BI-Ready Data Structures:** Delivering flat, denormalized views and pre-aggregated data marts tailored for seamless integration with dashboarding tools like Apache Superset to power dynamic visualizations.

### Technology Stack

* **ClickHouse:** The high-performance columnar data warehouse. (Already hosted externally via existing Docker container).
* **dbt (Data Build Tool):** Handles the SQL-based transformations to move data from Bronze -> Silver -> Gold.
* **Dagster:** The software-defined asset orchestrator. It triggers data ingestion, executes dbt models, and provides observability.
* **Apache Superset:** The open-source data exploration and visualization platform. Connects directly to the ClickHouse Gold layer.

### Data Flow Pipeline

`Raw CSV` --> `Dagster (Ingest)` --> **\[ClickHouse Bronze\]** --> `dbt (Clean)` --> **\[ClickHouse Silver\]** --> `dbt (Aggregate)` --> **\[ClickHouse Gold\]** --> **\[Apache Superset\]**

## 2. Prerequisites & Environment Setup

Ensure the following Python packages are installed in your virtual environment:

```bash
pip install dagster dagster-webserver dagster-dbt dbt-clickhouse pandas clickhouse-connect
```

**dbt Profile (`~/.dbt/profiles.yml`):**
Configure dbt to connect to your existing ClickHouse container. Note that we are using the container name (`clickhouse_server`) as the host since they will share a Docker network.

```yaml
earthquake_dbt:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: mydb
      host: clickhouse_server 
      port: 8123
      user: myuser
      password: 'mypassword'
```

## 3. Medallion Architecture Design

### Layer 1: Bronze (Raw Ingestion)
* **Goal:** Load `katalog_gempa.csv` into ClickHouse exactly as it is, with no transformations.

### Layer 2: Silver (Cleansing & Dimensional Modeling)
* **Goal:** Clean types, handle nulls, and create a Star Schema (`fact_earthquake`, `dim_location`).

### Layer 3: Gold (Analytics & Reporting)
* **Goal:** Pre-aggregated views and denormalized tables tailored for BI tools and Data Analysts.

## 4. Data Analyst Output Requirements

### A. Ad-Hoc Querying Layer (Star Schema)
Analysts require access to the cleanly modeled Silver/Gold tables (`fact_earthquake`, `dim_location`, `dim_date`) for complex, ad-hoc SQL querying.

### B. Denormalized "One Big Table" (OBT) Views
Superset and similar tools perform best with flat datasets. The pipeline must generate an OBT view that pre-joins all necessary context.
* **Output Target:** `view_earthquake_analytics_master`

### C. Pre-Aggregated Data Marts
* **Time-Series Mart (`mart_monthly_trends`):** Summarizes `total_earthquakes`, `avg_magnitude`, and `max_magnitude` grouped by month/year.
* **Geospatial Risk Mart (`mart_regional_risk`):** Summarizes event counts and average depths grouped by `region_name`.

### D. Data Quality & Freshness Metadata
* The pipeline must expose metadata to analysts indicating the **last successful refresh timestamp** and any **data quality test results**.

### E. Supported Visualization Goals
* **deck.gl Geospatial Heatmaps:** Mapping `latitude` and `longitude` alongside `magnitude`.
* **Time-Series Line Charts:** Tracking earthquake frequency and intensity trends.
* **Depth vs. Magnitude Scatter Plots:** Analyzing correlation between `depth_km` and `magnitude`.
* **Executive KPI Big Number Charts:** Displaying critical summary metrics.

## 5. Implementation Details

### Step A: dbt Project Structure

```text
models/
├── bronze/
│   └── source.yml 
├── silver/
│   ├── stg_earthquakes.sql
│   ├── dim_location.sql
│   └── fact_earthquake.sql
└── gold/
    ├── obt_earthquake_analytics.sql
    └── mart_monthly_trends.sql
```

### Step B: Dagster Implementation (`assets.py`)

Update your Dagster connection logic to use your existing ClickHouse credentials and container hostname.

```python
import pandas as pd
import clickhouse_connect
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_dbt import dbt_assets, DbtCliResource
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "earthquake_dbt").resolve()

@asset(compute_kind="python", group_name="bronze")
def raw_earthquakes():
    """Extracts CSV and loads it into ClickHouse Bronze layer."""
    df = pd.read_csv("katalog_gempa.csv")
    
    # Connect using existing container credentials
    client = clickhouse_connect.get_client(
        host='clickhouse_server', 
        port=8123, 
        username='myuser',
        password='mypassword'
    )
    client.command("CREATE DATABASE IF NOT EXISTS mydb")
    client.insert_df('mydb.raw_earthquakes', df)
    
    return MaterializeResult(
        metadata={
            "row_count": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

@dbt_assets(manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"))
def earthquake_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Executes dbt models to build Silver and Gold layers."""
    yield from dbt.cli(["build"], context=context).stream()
```

## 6. Data Quality & Testing Architecture

Defined in `models/schema.yml` using `dbt test` to ensure data integrity before it reaches Superset.

```yaml
version: 2
models:
  - name: silver_fact_earthquake
    columns:
      - name: magnitude
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0.0
              max_value: 10.0
```

## 7. Connecting to Existing ClickHouse & Local Deployment

Because ClickHouse is already running, you need to create a **shared Docker network** so Dagster and Superset can securely communicate with it using the hostname `clickhouse_server`.

### Step 1: Create a Shared Network
Open your terminal and create a new external network:
```bash
docker network create data_pipeline_net
```

### Step 2: Update your Existing ClickHouse Compose File
Add the external network to the `docker-compose.yml` file you already use for ClickHouse, then restart it (`docker-compose down && docker-compose up -d`).

```yaml
services:
  clickhouse_server:
    image: clickhouse/clickhouse-server:latest-alpine
    container_name: clickhouse_server
    environment:
      CLICKHOUSE_USER: myuser
      CLICKHOUSE_PASSWORD: mypassword
      CLICKHOUSE_DB: mydb
    ports:
      - "8123:8123" 
      - "9000:9000" 
    restart: always
    networks:
      - data_pipeline_net # Added this

networks:
  data_pipeline_net:
    external: true
```

### Step 3: Dagster & Superset `docker-compose.yml`
In your new project folder, create a `docker-compose.yml` specifically for the orchestration and visualization tools. It will connect to the same external network.

```yaml
version: '3.8'

services:
  # 1. Data Pipeline Orchestration (Dagster)
  dagster:
    image: python:3.10-slim
    container_name: dagster_orchestrator
    ports:
      - "3000:3000"
    environment:
      DAGSTER_HOME: /opt/dagster/dagster_home
    volumes:
      - ./dagster_project:/opt/dagster/app
    command: >
      bash -c "pip install dagster dagster-webserver dagster-dbt dbt-clickhouse dagster-clickhouse pandas clickhouse-connect &&
               dagster dev -h 0.0.0.0 -p 3000 -d /opt/dagster/app"
    networks:
      - data_pipeline_net

  # 2. Data Visualization Platform (Superset)
  superset:
    image: apache/superset:latest
    container_name: superset_viz
    ports:
      - "8088:8088"
    environment:
      SUPERSET_SECRET_KEY: 'your_secure_secret_key'
    volumes:
      - superset_home:/app/superset_home
    networks:
      - data_pipeline_net

networks:
  data_pipeline_net:
    external: true

volumes:
  superset_home:
```