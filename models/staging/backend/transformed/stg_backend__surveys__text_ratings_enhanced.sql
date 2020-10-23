
with
  text_ratings as (
    select * from {{ref('stg_backend__text__ratings')}})
  , users as (
    select * from {{ref('stg_backend__entities__users')}})
  , controllers as (
    select * from {{ref('stg_backend__entities__controllers')}})

select
  text_ratings.text_answer_id
  , text_ratings.text_id
  , text_ratings.text_category
  , text_ratings.page_id
  , text_ratings.tax_form_name
  , text_ratings.tax_year
  , text_ratings.text_is_helpful
  , text_ratings.text_is_well_translated
  , text_ratings.customer_action_id
  , text_ratings.user_id
  , date_diff(date(text_ratings.created_at), date(users.created_at), year) as user_seniority
  , case
      when date_diff(date(text_ratings.created_at), date(users.created_at), year) = 0 then 'new'
      when date_diff(date(text_ratings.created_at), date(users.created_at), year) > 0 then 'old'
    end as user_type
  , users.created_at as user_created_at
  , text_ratings.controller_id
  , controllers.brand_name
  , text_ratings.culture_short

  , text_ratings.created_at
  , date(text_ratings.created_at) as date_day
  , date_trunc (date(text_ratings.created_at), week(monday)) as date_week
  , extract(year from text_ratings.created_at) as date_year

  , length(text_ratings.text_answer_comment) as text_answer_comment_length
  , text_ratings.text_answer_comment

from text_ratings
  left join users on text_ratings.user_id = users.user_id
  left join controllers on text_ratings.controller_id = controllers.controller_id

order by text_ratings.text_answer_id asc
