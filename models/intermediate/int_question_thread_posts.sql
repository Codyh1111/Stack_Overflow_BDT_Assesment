with questions as (
  select post_id as question_id                         
  from {{ ref('stg_posts') }}
  where post_type_id = 1
),
answers as (
  select
    parent_id as question_id,
    post_id   as post_id
  from {{ ref('stg_posts') }}
  where post_type_id = 2
    and parent_id is not null
)
select question_id, question_id as post_id
from questions
union all
select question_id, post_id
from answers

