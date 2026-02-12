select
  Id as user_id,
  DisplayName as display_name
from {{ source('stackoverflow','users') }}
