
with
  nps_answers as (

    select *

    from {{ref('stg_backend__surveys__nps_answers')}}

  ),

  users as (

    select *

    from {{ref('stg_backend__entities__users')}}

  ),

  modules as (

    select *

    from {{ref('stg_backend__surveys__nps_modules')}}

  ),

  controllers as (

    select *

    from {{ref('stg_backend__entities__controllers')}}

  )

select
  nps_answers.nps_answer_id,
  nps_answers.nps_module_id,
  modules.module_text,
  nps_answers.user_id,
  date_diff(current_date(), date(users.created_at), year) as user_seniority,
  case
    when date_diff(current_date(), date(users.created_at), year) = 0 then 'new'
    when date_diff(current_date(), date(users.created_at), year) > 0 then 'old'
  end as user_type,
  users.created_at as user_created_at,
  nps_answers.nps_answer_score,
  case
    when nps_answers.nps_answer_score > 8 then 100
    when nps_answers.nps_answer_score > 6 then 0
    when nps_answers.nps_answer_score > 0 then -100
  end as nps_weight,
  nps_answers.controller_id,
  controllers.brand_name,
  nps_answers.culture_short,
  nps_answers.created_at,
  nps_answers.nps_answer_comment

from nps_answers
  left join users on nps_answers.user_id = users.user_id
  left join modules on nps_answers.nps_module_id = modules.nps_module_id
  left join controllers on nps_answers.controller_id = controllers.controller_id

order by nps_answers.nps_answer_id asc
