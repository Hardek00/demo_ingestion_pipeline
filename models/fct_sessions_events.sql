-- Fact: events from JSON column
{{ config(materialized='table') }}

select * from {{ ref('stg_sessions_events') }}
