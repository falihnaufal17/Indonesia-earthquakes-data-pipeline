{{ config(alias='meta_data_freshness', engine='MergeTree()', order_by='refreshed_at') }}

-- Single-row freshness/volume metadata for analysts. `refreshed_at` is the time
-- this dbt run executed, so it only advances when the pipeline succeeds.
select
    now()                                                  as refreshed_at,
    (select max(ingested_at) from {{ ref('fact_earthquake') }}) as last_ingested_at,
    min(event_time)                                        as earliest_event_time,
    max(event_time)                                        as latest_event_time,
    count()                                                as fact_rows,
    (select count() from {{ source('bronze', 'raw_earthquakes') }}) as bronze_rows,
    bronze_rows - fact_rows                                as rows_rejected_or_deduplicated
from {{ ref('fact_earthquake') }}
