{{ config(materialized='view') }}

WITH src AS (
  SELECT raw_json
  FROM {{ source('raw', 'excersise_1_customers') }}
),
arr AS (
  SELECT PARSE_JSON(x) AS obj
  FROM src, UNNEST(JSON_QUERY_ARRAY(raw_json, '$')) AS x
)
SELECT
  CAST(JSON_VALUE(obj, '$.id') AS INT64)         AS customer_id,
  INITCAP(JSON_VALUE(obj, '$.name'))             AS customer_name,
  LOWER(JSON_VALUE(obj, '$.email'))              AS email,
  CAST(JSON_VALUE(obj, '$.signup_date') AS DATE) AS signup_date
FROM arr;