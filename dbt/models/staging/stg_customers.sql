
{{ config(materialized='view') }}


from {{ source('raw', 'excersise_2_customers') }}
