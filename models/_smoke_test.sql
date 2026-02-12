select count(*) as post_count
from {{ source('stackoverflow', 'posts') }}