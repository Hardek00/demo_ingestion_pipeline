
{{ config(materialized='view') }}

select
  cast(id as int64) as customer_id,
  initcap(name) as customer_name,
  lower(email) as email,
  cast(signup_date as date) as signup_date
from {{ source('raw', 'excersise_2_customers') }}
