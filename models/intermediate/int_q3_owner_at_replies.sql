with questions as (
  select
    post_id as question_id,                     -- we need this to join to thread posts and comments
    owner_user_id as question_owner_user_id     -- we need this to join to comments and check if the comment author is the same as the question owner
  from {{ ref('stg_posts') }}                     
  where post_type_id = 1                        -- filter to only questions
    and owner_user_id is not null                    
),
thread_posts as (
  select * from {{ ref('int_question_thread_posts') }}  
),
owner_comments as (
  select
    tp.question_id,             -- we need this to group by question and count the number of comments per question
    c.comment_id,                 -- we need this to count the number of comments per question
    c.user_id,              -- we need this to check if the comment author is the same as the question owner
    c.comment_text                      -- we need this to check if the comment starts with @   
  from thread_posts tp                  
  join {{ ref('stg_comments') }} c      
    on c.post_id = tp.post_id
  join questions q      
    on q.question_id = tp.question_id
   and q.question_owner_user_id = c.user_id
  where regexp_matches(c.comment_text, '^\\s*@')  -- search for comments that start with @, allowing for leading whitespace before the @
)
select
  question_id
from owner_comments             -- look at all comments that are made by the question owner on their own question thread
group by 1                      -- group by question to count the number of comments per question
