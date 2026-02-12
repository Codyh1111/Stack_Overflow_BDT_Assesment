select
  p.owner_user_id as user_id, 
  u.display_name,
  sum(coalesce(p.view_count, 0)) as total_question_views,       -- total views for all questions asked by the user
  count(distinct p.post_id) as total_questions_asked             -- total number of questions
from {{ ref('stg_posts') }} p
left join {{ ref('stg_users') }} u
  on u.user_id = p.owner_user_id
where p.post_type_id = 1
  and p.owner_user_id is not null
group by 1, 2                                                     -- group by user_id and display_name to get the total views and total questions asked per user
order by total_question_views desc
limit 10