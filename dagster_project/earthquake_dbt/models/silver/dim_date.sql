{{ config(
    alias='silver_dim_date',
    engine='MergeTree()',
    order_by='date_key'
) }}

-- One row per calendar day between the first and last observed event.
with bounds as (
    select toDate(min(event_time)) as d_min, toDate(max(event_time)) as d_max
    from {{ ref('stg_earthquakes') }}
),

days as (
    select addDays(d_min, number) as date_key
    from bounds
    array join range(toUInt32(dateDiff('day', d_min, d_max)) + 1) as number
)

select
    date_key,
    toYear(date_key)                       as year,
    toQuarter(date_key)                    as quarter,
    toMonth(date_key)                      as month,
    arrayElement(['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 'Juli',
                  'Agustus', 'September', 'Oktober', 'November', 'Desember'],
                 toMonth(date_key))        as month_name,
    toStartOfMonth(date_key)               as month_start,
    formatDateTime(date_key, '%Y-%m')      as year_month,
    toDayOfMonth(date_key)                 as day_of_month,
    toDayOfWeek(date_key)                  as day_of_week,
    arrayElement(['Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu', 'Minggu'],
                 toDayOfWeek(date_key))    as day_of_week_name,
    toDayOfWeek(date_key) in (6, 7)        as is_weekend
from days
