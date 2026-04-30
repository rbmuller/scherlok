select
    id,
    name,
    email,
    plan,
    created_at
from {{ source('demo', 'users') }}
