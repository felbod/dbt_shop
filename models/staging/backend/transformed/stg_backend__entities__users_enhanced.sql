
with
  users as (

    select *

    from {{ref('stg_backend__entities__users')}}

  )

  , customers as (

    select *

    from {{ref('stg_backend__entities__customers')}}

  )

select
  users.user_id
  , users.controller_id
  , users.created_at
  , customers.customer_id

from users
  left join customers on customers.user_id = users.user_id

order by users.created_at desc
