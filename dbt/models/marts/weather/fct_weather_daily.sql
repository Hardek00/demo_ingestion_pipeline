-- Purpose: daily temps per location (built from the staging model)

-- BigQuery table config:
--  - materialized table (fast to query)
--  - partition by day for pruning
--  - cluster by city (optional)
{{ config(
    materialized = 'table',
    partition_by = {'field': 'day_utc', 'data_type': 'date'},
    cluster_by   = ['city']
) }}

with hourly as (
  select
    city, country, lat, lon,
    date(observed_at) as day_utc,   -- if your staging uses observed_at_utc, change this line
    temp_c
  from {{ ref('stg_weather_raw') }}
)

select
  -- one row per location+day
  concat(cast(lat as string), ',', cast(lon as string), '|', cast(day_utc as string)) as weather_day_key,
  city,
  country,
  lat,
  lon,
  day_utc,
  count(*)  as hours_count,
  avg(temp_c) as avg_temp_c,
  min(temp_c) as min_temp_c,
  max(temp_c) as max_temp_c
from hourly
group by 1,2,3,4,5,6
