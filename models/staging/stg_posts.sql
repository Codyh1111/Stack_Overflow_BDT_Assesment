select
  Id as post_id,
  PostTypeId as post_type_id,
  AcceptedAnswerId as accepted_answer_id,
  ParentId as parent_id,
  OwnerUserId as owner_user_id,
  CreationDate as created_at,
  ViewCount as view_count,
  Score as score
from {{ source('stackoverflow','posts') }}
