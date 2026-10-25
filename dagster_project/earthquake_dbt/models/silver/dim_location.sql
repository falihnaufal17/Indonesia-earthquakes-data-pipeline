{{ config(
    alias='silver_dim_location',
    engine='MergeTree()',
    order_by='location_key'
) }}

select
    {{ location_key('region_name') }} as location_key,
    region_name,
    round(avg(latitude), 4)  as centroid_latitude,
    round(avg(longitude), 4) as centroid_longitude
from {{ ref('stg_earthquakes') }}
group by region_name
