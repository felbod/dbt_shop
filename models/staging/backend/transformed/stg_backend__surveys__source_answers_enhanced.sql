
with
  source_answers as (
    select * from {{ref('stg_backend__surveys__source_answers')}})
  , users as (
    select * from {{ref('stg_backend__entities__users')}})
  , options as (
    select * from {{ref('stg_backend__surveys__source_options')}})
  , controllers as (
    select * from {{ref('stg_backend__entities__controllers')}})

select
  source_answers.user_id
  , date_diff(date(source_answers.created_at), date(users.created_at), year) as user_seniority
  , case
      when date_diff(date(source_answers.created_at), date(users.created_at), year) = 0 then 'new'
      when date_diff(date(source_answers.created_at), date(users.created_at), year) > 0 then 'old'
    end as user_type
  , users.created_at as user_created_at
  , case
      when source_answers.source_option_id < 100 then 'source'
      when source_answers.source_option_id >= 100 then 'corona-aid'
    end as survey_type
  , source_answers.source_option_id
  , options.option_text
  , source_answers.customer_action_id
  , length(source_answers.source_answer_comment) as source_answer_comment_length
  , source_answers.source_answer_comment
  , source_answers.controller_id
  , controllers.brand_name
  , source_answers.culture_short
  , source_answers.created_at
  , date(source_answers.created_at) as date_day
  , date_trunc (date(source_answers.created_at), week(monday)) as date_week
--  , date_trunc (date(source_answers.created_at), year) as date_year -- Alternative mit "1.1." als Zusatz bei Jahr
  , extract (year from source_answers.created_at) as date_year

from source_answers
  left join users on source_answers.user_id = users.user_id
  left join options on source_answers.source_option_id = options.source_option_id
  left join controllers on source_answers.controller_id = controllers.controller_id

order by source_answers.created_at desc
