-- Staging: flatten orders.items
{{ config(materialized='view') }}

with src as (
  select * from {{ source('raw', 'orders') }}
),
exploded as (
  select
    order_id,
    customer_id,
    cast(order_date as timestamp) as order_ts,
    item.product_id as product_id,
    item.quantity as quantity,
    item.unit_price as unit_price,
    item.quantity * item.unit_price as line_amount
  from src, unnest(items) as item
)
select * from exploded
