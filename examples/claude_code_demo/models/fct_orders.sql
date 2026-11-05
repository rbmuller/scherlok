-- One row per order, with the region of the customer who placed it.
select
    o.order_id,
    o.customer_id,
    o.ordered_at,
    o.amount,
    c.region
from {{ ref('stg_orders') }} as o
left join {{ ref('customers') }} as c
    on c.customer_id = o.customer_id
