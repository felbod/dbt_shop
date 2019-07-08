
with backend_revenue as (

  select *

    from {{ref('stg_backend__revenue_customer_type')}}

),

backend_sales as (

  select *

    from {{ref('stg_backend__sales_customer_type')}}

),

backend_signups as (

  select *

    from {{ref('stg_backend__signups_customer_type')}}

),

backend_starts as (

  select *

    from {{ref('stg_backend__starts_customer_type')}}

)

select
  backend_revenue.account_name,
  backend_revenue.date_day,
  sum(backend_revenue.revenue_old_customers) as revenue_old_customers,
  sum(backend_revenue.revenue_new_customers) as revenue_new_customers,

  sum(backend_sales.sales_old_customers) as sales_old_customers,
  sum(backend_sales.sales_new_customers) as sales_new_customers,

  sum(backend_starts.starts_old_customers) as starts_old_customers,
  sum(backend_starts.starts_new_customers) as starts_new_customers,

  sum(backend_signups.signups) as signups

from backend_revenue
  left join backend_sales on backend_revenue.date_day = backend_sales.date_day
    and backend_revenue.account_name = backend_sales.account_name
  left join backend_signups on backend_revenue.date_day = backend_signups.date_day
    and backend_revenue.account_name = backend_signups.account_name
  left join backend_starts on backend_revenue.date_day = backend_starts.date_day
    and backend_revenue.account_name = backend_starts.account_name

group by
  backend_revenue.account_name,
  backend_revenue.date_day
