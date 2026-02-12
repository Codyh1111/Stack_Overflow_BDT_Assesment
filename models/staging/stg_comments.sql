select
  Id as comment_id,
  PostId as post_id,
  UserId as user_id,
  CreationDate as created_at,
  Text as comment_text
from {{ source('stackoverflow','comments') }}
