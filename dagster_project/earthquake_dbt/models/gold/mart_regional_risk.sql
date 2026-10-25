{{ config(
    alias='mart_regional_risk',
    engine='MergeTree()',
    order_by='region_name'
) }}

select
    l.region_name                                   as region_name,
    any(l.centroid_latitude)                        as centroid_latitude,
    any(l.centroid_longitude)                       as centroid_longitude,
    count()                                         as total_earthquakes,
    round(avg(f.depth_km), 1)                       as avg_depth_km,
    round(avg(f.magnitude), 2)                      as avg_magnitude,
    max(f.magnitude)                                as max_magnitude,
    countIf(f.magnitude >= 5)                       as significant_earthquakes,
    round(countIf(f.depth_km < 70) / count() * 100, 1) as pct_shallow
from {{ ref('fact_earthquake') }} as f
inner join {{ ref('dim_location') }} as l on f.location_key = l.location_key
group by l.region_name
