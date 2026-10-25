{{ config(materialized='view', alias='silver_stg_earthquakes') }}

-- Cast types, null-handle, drop unusable rows and de-duplicate repeated events.
with typed as (

    select
        parseDateTime64BestEffortOrNull(concat(replaceAll(tgl, '/', '-'), ' ', ot), 3, 'UTC') as event_time,
        toFloat64OrNull(lat)    as latitude,
        toFloat64OrNull(lon)    as longitude,
        toFloat64OrNull(depth)  as depth_km,
        toFloat64OrNull(mag)    as magnitude,
        coalesce(nullIf(trim(remark), ''), 'Unknown') as region_name,
        toFloat32OrNull(strike1) as strike_np1,
        toFloat32OrNull(dip1)    as dip_np1,
        toFloat32OrNull(rake1)   as rake_np1,
        toFloat32OrNull(strike2) as strike_np2,
        toFloat32OrNull(dip2)    as dip_np2,
        toFloat32OrNull(rake2)   as rake_np2,
        _ingested_at             as ingested_at
    from {{ source('bronze', 'raw_earthquakes') }}

),

valid as (

    select *
    from typed
    where event_time is not null
      and latitude  between -90  and 90
      and longitude between -180 and 180
      and magnitude is not null
      and depth_km  is not null and depth_km >= 0

),

keyed as (

    -- Rows are validated above, so the Nullable wrapper can be dropped here
    -- (needed for the sorting keys of the downstream tables).
    select
        assumeNotNull(event_time) as event_time,
        assumeNotNull(latitude)   as latitude,
        assumeNotNull(longitude)  as longitude,
        assumeNotNull(depth_km)   as depth_km,
        assumeNotNull(magnitude)  as magnitude,
        region_name,
        strike_np1, dip_np1, rake_np1,
        strike_np2, dip_np2, rake_np2,
        ingested_at,
        cityHash64(toString(event_time), latitude, longitude) as earthquake_id,
        row_number() over (
            partition by event_time, latitude, longitude
            order by magnitude desc, depth_km desc
        ) as dup_rank
    from valid

)

select * except (dup_rank)
from keyed
where dup_rank = 1
