
with
  trustedshops_answers as (

    select *

    from {{ref('stg_backend__surveys__trustedshops_answers')}}

  )

  , users as (

    select *

    from {{ref('stg_backend__entities__users_enhanced')}}

  )

  , controllers as (

    select *

    from {{ref('stg_backend__entities__controllers')}}

  )

select
  trustedshops_answers.trustedshops_answer_id
  , trustedshops_answers.controller_id
  , controllers.brand_name
  , trustedshops_answers.customer_id
  , date_diff(date(trustedshops_answers.created_at), date(users.created_at), year) as user_seniority
  , case
      when date_diff(date(trustedshops_answers.created_at), date(users.created_at), year) = 0 then 'new'
      when date_diff(date(trustedshops_answers.created_at), date(users.created_at), year) > 0 then 'old'
    end as user_type
  , users.created_at as user_created_at
  , trustedshops_answers.customer_action_id
  , trustedshops_answers.trustedshops_answer_score
  , trustedshops_answers.trustedshops_order_reference
  , if(
      REGEXP_CONTAINS(trustedshops_answers.trustedshops_order_reference, '[0-9]{1,}[a-z]{1}$'),
        REGEXP_REPLACE(trustedshops_answers.trustedshops_order_reference, '([0-9]{1,})([a-z]{1})', '\\2'),
        '')
      as trustedshops_reference
  , trustedshops_answers.created_at
  , date(trustedshops_answers.created_at) as date_day
  , extract (year from trustedshops_answers.created_at) as date_year
  , length(trustedshops_answers.trustedshops_answer_comment) as trustedshops_answer_comment_length
  , trustedshops_answers.trustedshops_answer_comment

from trustedshops_answers
  left join users on trustedshops_answers.customer_id = users.customer_id
  left join controllers on trustedshops_answers.controller_id = controllers.controller_id

order by trustedshops_answers.created_at desc
