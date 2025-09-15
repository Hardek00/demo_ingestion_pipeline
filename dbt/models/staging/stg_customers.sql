
{{ config(materialized='view') }}

select
Cast(name as string) as full_name,
cast(signup_date as date) as signup

from {{ source('raw', 'excersise_2_customers') }}
