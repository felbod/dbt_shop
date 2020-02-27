
select
  cast(date_day as date) as date_day,
  controller_id,
  sales_old_customers,
  sales_new_customers,

  /* Revenues from backend:
  - Reported as gross values in Euro
  - Updated every weekday at 1500 hours to include most prepayments from previous day
  - Includes values for previous day */
  revenue_old_customers / 1.19 as revenue_net_eur_old_customers,
  revenue_new_customers / 1.19 as revenue_net_eur_new_customers

from `planar-depth-242012.backend_data.backend__conversions__sales`
