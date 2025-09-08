{{ config(materialized='view') }}

{{ dbt_utils.deduplicate(
    relation=ref('stg_customers_raw'),
    partition_by='customer_id',
    order_by='loaded_at desc, customer_id'   
) }}
