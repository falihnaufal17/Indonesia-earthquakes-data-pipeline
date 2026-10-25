{{ config(
    alias='mart_monthly_trends',
    engine='MergeTree()',
    order_by='month_start'
) }}

select
    d.month_start                      as month_start,
    d.year                             as year,
    d.month                            as month,
    count()                            as total_earthquakes,
    round(avg(f.magnitude), 2)         as avg_magnitude,
    max(f.magnitude)                   as max_magnitude,
    round(avg(f.depth_km), 1)          as avg_depth_km,
    countIf(f.magnitude >= 5)          as significant_earthquakes
from {{ ref('fact_earthquake') }} as f
inner join {{ ref('dim_date') }} as d on f.date_key = d.date_key
group by d.month_start, d.year, d.month
