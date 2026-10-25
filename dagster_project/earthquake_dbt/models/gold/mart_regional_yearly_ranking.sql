{{ config(
    alias='mart_regional_yearly_ranking',
    engine='MergeTree()',
    order_by='(year, rank_in_year)'
) }}

-- Regions ranked by earthquake count within each year (rank 1 = most earthquakes).
select
    year,
    region_name,
    total_earthquakes,
    avg_magnitude,
    max_magnitude,
    significant_earthquakes,
    rank() over (partition by year order by total_earthquakes desc) as rank_in_year
from (
    select
        d.year                       as year,
        l.region_name                as region_name,
        count()                      as total_earthquakes,
        round(avg(f.magnitude), 2)   as avg_magnitude,
        max(f.magnitude)             as max_magnitude,
        countIf(f.magnitude >= 5)    as significant_earthquakes
    from {{ ref('fact_earthquake') }} as f
    inner join {{ ref('dim_location') }} as l on f.location_key = l.location_key
    inner join {{ ref('dim_date') }}     as d on f.date_key = d.date_key
    group by d.year, l.region_name
)
