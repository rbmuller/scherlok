select
    user_id,
    count(*) as order_count,
    sum(amount) as total_amount,
    max(created_at) as last_order_at
from {{ source('demo', 'orders') }}
group by user_id
