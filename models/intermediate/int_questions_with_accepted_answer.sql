with questions as (
  select
    post_id as question_id,
    created_at as question_created_at,                
    accepted_answer_id
  from {{ ref('stg_posts') }}
  where post_type_id = 1                                  --filter to only questions        
    and accepted_answer_id is not null                    --filter to only questions with an accepted answer
    and created_at is not null                            --filter to only questions with a created_at timestamp
answers as (
  select
    post_id as answer_id,
    created_at as answer_created_at
  from {{ ref('stg_posts') }}
  where post_type_id = 2                                  --filter to only answers
    and created_at is not null
)
select
  q.question_id,
  q.question_created_at,
  q.accepted_answer_id,
  a.answer_created_at as accepted_answer_created_at,
  date_diff('second', q.question_created_at, a.answer_created_at) as secs_to_accepted       --calculate time to accepted answer
from questions q
join answers a                                                                               
  on a.answer_id = q.accepted_answer_id
where date_diff('second', q.question_created_at, a.answer_created_at) >= 0                 --filter to only questions where the accepted answer was created after the question  
