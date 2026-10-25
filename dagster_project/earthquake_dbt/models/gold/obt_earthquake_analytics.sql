{{ config(materialized='view', alias='view_earthquake_analytics_master') }}

-- One Big Table for Superset: fact + every dimension, pre-joined and flat.
select
    f.earthquake_id     as earthquake_id,
    f.event_time        as event_time,
    f.date_key          as event_date,
    d.year              as year,
    d.quarter           as quarter,
    d.month             as month,
    d.month_name        as month_name,
    d.month_start       as month_start,
    d.year_month        as year_month,
    d.day_of_week_name  as day_of_week_name,
    d.is_weekend        as is_weekend,
    l.region_name       as region_name,
    f.latitude          as latitude,
    f.longitude         as longitude,
    f.depth_km          as depth_km,
    f.magnitude         as magnitude,
    f.magnitude_class   as magnitude_class,
    f.depth_class       as depth_class,
    f.ingested_at       as ingested_at
from {{ ref('fact_earthquake') }} as f
inner join {{ ref('dim_location') }} as l on f.location_key = l.location_key
inner join {{ ref('dim_date') }}     as d on f.date_key = d.date_key
