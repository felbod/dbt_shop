
with
  trustedshops_answers as (

    select *

    from {{ref('stg_backend__surveys__trustedshops_answers')}}

  ),

  users as (

    select *

    from {{ref('stg_backend__entities__users')}}

  ),

  controllers as (

    select *

    from {{ref('stg_backend__entities__controllers')}}

  )

select
  trustedshops_answers.trustedshops_answer_id,
  trustedshops_answers.controller_id,
  controllers.brand_name,
  trustedshops_answers.user_id,
  date_diff(date(trustedshops_answers.created_at), date(users.created_at), year) as user_seniority,
  case
    when date_diff(date(trustedshops_answers.created_at), date(users.created_at), year) = 0 then 'new'
    when date_diff(date(trustedshops_answers.created_at), date(users.created_at), year) > 0 then 'old'
  end as user_type,
  users.created_at as user_created_at,
  trustedshops_answers.customer_action_id,
  trustedshops_answers.trustedshops_answer_score,
  trustedshops_answers.trustedshops_order_reference,
  trustedshops_answers.trustedshops_answer_comment,
  trustedshops_answers.created_at

from trustedshops_answers
  left join users on trustedshops_answers.user_id = users.user_id
  left join controllers on trustedshops_answers.controller_id = controllers.controller_id

order by trustedshops_answers.created_at desc
