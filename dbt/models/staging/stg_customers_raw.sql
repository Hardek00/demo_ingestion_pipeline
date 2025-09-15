{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('raw', 'excersise_1_customers') }}
),


