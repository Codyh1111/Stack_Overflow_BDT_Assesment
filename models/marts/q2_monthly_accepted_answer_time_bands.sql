with base as (
  select
    date_trunc('month', question_created_at) as month_start,                    -- we will use this to group by month and calculate the monthly distribution of accepted answer times
    secs_to_accepted,
    case
      when secs_to_accepted < 60 then '<1 min'
      when secs_to_accepted < 5*60 then '1-5 mins'
      when secs_to_accepted < 60*60 then '5 mins-1 hour'
      when secs_to_accepted < 3*60*60 then '1-3 hours'
      when secs_to_accepted < 24*60*60 then '3 hours-1 day'
      when secs_to_accepted < 3*24*60*60 then '1-3 days'
      when secs_to_accepted < 7*24*60*60 then '3-7 days'
      else '7+ days'
    end as band
  from {{ ref('int_questions_with_accepted_answer') }}
)
select
  strftime(month_start, '%Y-%m') as month,                                              -- format the month_start as 'YYYY-MM' for better readability in the final output
  sum(case when band = '<1 min' then 1 else 0 end) as "<1 min",                         -- create Bands
  sum(case when band = '1-5 mins' then 1 else 0 end) as "1-5 mins",
  sum(case when band = '5 mins-1 hour' then 1 else 0 end) as "5 mins-1 hour",
  sum(case when band = '1-3 hours' then 1 else 0 end) as "1-3 hours",
  sum(case when band = '3 hours-1 day' then 1 else 0 end) as "3 hours-1 day",
  sum(case when band = '1-3 days' then 1 else 0 end) as "1-3 days",
  sum(case when band = '3-7 days' then 1 else 0 end) as "3-7 days",
  sum(case when band = '7+ days' then 1 else 0 end) as "7+ days",
  count(*) as total_accepted_answer
from base
group by 1, month_start 
order by month_start
