
with
  backend_sales as (
    select * from {{ref('stg_backend__conversions__sales_revenue')}})
  , backend_signups as (
    select * from {{ref('stg_backend__conversions__signups')}})
  , backend_starts as (
    select * from {{ref('stg_backend__conversions__starts')}})
  , controllers as (
    select * from {{ref('stg_backend__entities__controllers')}})

select
  backend_sales.date_day
  , extract (year from backend_sales.date_day) as date_year
  , extract (isoyear from backend_sales.date_day) as date_isoyear
  , extract (isoweek from backend_sales.date_day) as date_isoweek
  , date_trunc (backend_sales.date_day, week(monday)) as date_week
  , backend_sales.controller_id
  , controllers.brand_name
  , (backend_sales.revenue_net_eur__old_customers
    + backend_sales.revenue_net_eur__new_customers)
    as revenue_net_eur
  , backend_sales.revenue_net_eur__old_customers as revenue_net_eur__old_customers
  , backend_sales.revenue_net_eur__new_customers as revenue_net_eur__new_customers
  , (backend_sales.sales__old_customers
    + backend_sales.sales__new_customers)
    as sales
  , backend_sales.sales__old_customers as sales__old_customers
  , backend_sales.sales__new_customers as sales__new_customers
  , (backend_starts.starts__old_customers
    + backend_starts.starts__new_customers)
    as starts
  , backend_starts.starts__old_customers as starts__old_customers
  , backend_starts.starts__new_customers as starts__new_customers
  , backend_signups.signups__new_customers as signups__new_customers
  , safe_divide(
      backend_sales.sales__old_customers,
      backend_starts.starts__old_customers)
      as sale_rate__old_customers
  , safe_divide(
      backend_sales.sales__new_customers,
      backend_starts.starts__new_customers)
      as sale_rate__new_customers
  , safe_divide(
      backend_starts.starts__new_customers,
      backend_signups.signups__new_customers)
      as start_rate__new_customers
-- to do: erst avg bilden, dann teilen
  , AVG(safe_divide(backend_starts.starts__new_customers, backend_signups.signups__new_customers))
    OVER (
      partition by backend_sales.controller_id
      ORDER BY backend_sales.date_day asc
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    as start_rate__new_customers__7_ma

from backend_sales
  left join backend_signups on backend_sales.date_day = backend_signups.date_day
    and backend_sales.controller_id = backend_signups.controller_id
  left join backend_starts on backend_sales.date_day = backend_starts.date_day
    and backend_sales.controller_id = backend_starts.controller_id
  left join controllers on backend_sales.controller_id = controllers.controller_id

  order by
    date_day desc
    , controller_id
