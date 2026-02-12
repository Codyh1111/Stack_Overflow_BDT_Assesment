select
  count(*) as questions_with_owner_at_replies
from {{ ref('int_q3_owner_at_replies') }}
