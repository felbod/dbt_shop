
with
  source_answers as (

    select *

    from {{ref('stg_backend__surveys__source_answers')}}

  ),

  users as (

    select *

    from {{ref('stg_backend__entities__users')}}

  ),

  options as (

    select *

    from {{ref('stg_backend__surveys__source_options')}}

  ),

  controllers as (

    select *

    from {{ref('stg_backend__entities__controllers')}}

  )

select
  source_answers.user_id,
  date_diff(current_date(), date(users.created_at), year) as user_seniority,
  users.created_at as user_created_at,
  source_answers.source_option_id,
  options.option_text,
  source_answers.customer_action_id,
  source_answers.source_answer_comment,
  source_answers.controller_id,
  controllers.brand_name,
  source_answers.culture_short,
  source_answers.created_at

from source_answers
  left join users on source_answers.user_id = users.user_id
  left join options on source_answers.source_option_id = options.source_option_id
  left join controllers on source_answers.controller_id = controllers.controller_id

order by source_answers.created_at desc
