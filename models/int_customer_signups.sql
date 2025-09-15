-- Intermediate: daily signups
{{ config(materialized='table') }}

with base as (
  select signup_date, customer_id
  from {{ ref('stg_customers') }}
)
select
  signup_date,
  count(distinct customer_id) as daily_signups
from base
group by 1
order by 1
