
with backend_sales as (

  select *

    from {{ref('stg_backend__conversions__sales')}}

),

backend_signups as (

  select *

    from {{ref('stg_backend__conversions__signups')}}

),

backend_starts as (

  select *

    from {{ref('stg_backend__conversions__starts')}}

),

controllers as (

  select *

  from {{ref('stg_backend__controllers')}}

)

select
  backend_sales.date_day,
  backend_sales.controller_id,
  controllers.brand_name,
  sum(backend_sales.revenue_net_eur_old_customers) as revenue_net_eur_old_customers,
  sum(backend_sales.revenue_net_eur_new_customers) as revenue_net_eur_new_customers,
  sum(backend_sales.sales_old_customers) as sales_old_customers,
  sum(backend_sales.sales_new_customers) as sales_new_customers,
  sum(backend_starts.starts_old_customers) as starts_old_customers,
  sum(backend_starts.starts_new_customers) as starts_new_customers,
  sum(backend_signups.signups_new_customers) as signups_new_customers

from backend_sales
  left join backend_signups on backend_sales.date_day = backend_signups.date_day
    and backend_sales.controller_id = backend_signups.controller_id
  left join backend_starts on backend_sales.date_day = backend_starts.date_day
    and backend_sales.controller_id = backend_starts.controller_id
  left join controllers on backend_sales.controller_id = controllers.controller_id

group by
  backend_sales.date_day,
  backend_sales.controller_id,
  controllers.brand_name
