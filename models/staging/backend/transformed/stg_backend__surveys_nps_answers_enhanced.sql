
with
  nps_answers as (

    select *

    from {{ref('stg_backend__surveys_nps_answers')}}

  ),

  users as (

    select *

    from {{ref('stg_backend__users')}}

  ),

  questions as (

    select *

    from `planar-depth-242012.backend_constants.backend_code_nps_question`

  )

select
  date_diff(current_date(), datetime users.created_at, year) as customer_seniority,
  nps_answers.nps_answer_id,
  nps_answers.nps_questions_id,
  questions.question as question_text,
  nps_answers.user_id,
  nps_answers.nps_answer_score,
  case
    when nps_answers.nps_answer_score > 8 then 100
    when nps_answers.nps_answer_score > 6 then 0
    when nps_answers.nps_answer_score > 0 then -100
  end as nps_weight,
  nps_answers.controller_id,
  nps_answers.culture_short,
  nps_answers.created_at,
  users.created_at as user_created_at,
  nps_answers.nps_answer_text

from nps_answers
  left join users on nps_answers.user_id = users.user_id
  left join questions on nps_answers.nps_questions_id = questions.id

order by nps_answers.nps_answer_id asc
