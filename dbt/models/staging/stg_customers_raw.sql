{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('raw', 'excersise_1_customers') }}
),

exploded as (
  select
    obj
  from src
  cross join unnest(json_query_array(raw_json, '$')) as obj
),

parsed as (
  select
    safe_cast(json_value(obj, '$.id') as int64)        as customer_id,
    initcap(json_value(obj, '$.name'))                 as customer_name,
    lower(json_value(obj, '$.email'))                  as customer_email,
    safe_cast(json_value(obj, '$.signup_date') as date) as signup_date
  from exploded
)

select *
from parsed
