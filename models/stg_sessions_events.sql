-- Staging: JSON column using JSONPath ($)
-- Assume a raw table raw.sessions_json with a single JSON column named 'raw'
{{ config(materialized='view') }}

with src as (
  select raw from {{ source('raw', 'sessions_json') }}
),
-- Extract arrays using JSON_VALUE/JSON_QUERY and JSONPath
root as (
  select raw,
         json_value(raw, '$.user_id') as user_id_str,
         json_query(raw, '$.sessions') as sessions_json
  from src
),
sessions as (
  -- Turn sessions array into rows
  select
    cast(user_id_str as int64) as user_id,
    session_obj
  from root, unnest(json_query_array(root.sessions_json)) as session_obj
),
events as (
  -- Turn each session's events into rows
  select
    user_id,
    json_value(session_obj, '$.session_id') as session_id,
    event_obj
  from sessions, unnest(json_query_array(session_obj, '$.events')) as event_obj
)
select
  user_id,
  session_id,
  json_value(event_obj, '$.event_type') as event_type,
  cast(json_value(event_obj, '$.ts') as timestamp) as event_ts
from events
