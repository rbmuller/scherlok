select
    id,
    name,
    price,
    category,
    stock,
    updated_at
from {{ source('demo', 'products') }}
