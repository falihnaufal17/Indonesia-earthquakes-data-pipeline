{{ config(
    alias='silver_fact_earthquake',
    engine='MergeTree()',
    order_by='(date_key, event_time)'
) }}

select
    earthquake_id,
    event_time,
    toDate(event_time)                       as date_key,
    {{ location_key('region_name') }}        as location_key,
    latitude,
    longitude,
    depth_km,
    magnitude,
    multiIf(magnitude < 3, 'Mikro (<3)',
            magnitude < 4, 'Kecil (3-3,9)',
            magnitude < 5, 'Ringan (4-4,9)',
            magnitude < 6, 'Sedang (5-5,9)',
            magnitude < 7, 'Kuat (6-6,9)',
                           'Besar (7+)')     as magnitude_class,
    multiIf(depth_km < 70,  'Dangkal (<70 km)',
            depth_km < 300, 'Menengah (70-300 km)',
                            'Dalam (300+ km)') as depth_class,
    strike_np1, dip_np1, rake_np1,
    strike_np2, dip_np2, rake_np2,
    ingested_at
from {{ ref('stg_earthquakes') }}
