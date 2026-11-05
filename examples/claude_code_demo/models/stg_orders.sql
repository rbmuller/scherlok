{{ config(materialized='view') }}

-- One row per order.
select
    order_id,
    customer_id,
    cast(ordered_at as date) as ordered_at,
    amount
from {{ ref('orders') }}
