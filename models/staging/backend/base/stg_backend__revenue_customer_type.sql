
with

  revenue_customers as (

  select
    'lohnsteuer-kompakt.de' as account_name,
    date as date_day,
    lk_gross_revenue_old_customers as gross_revenue_old_customers,
    lk_gross_revenue_new_customers as gross_revenue_new_customers
  from
    `planar-depth-242012.uploads.backend_revenue`

  union all

  select
    'steuergo.de' as account_name,
    date as date_day,
    sg_gross_revenue_old_customers as gross_revenue_old_customers,
    sg_gross_revenue_new_customers as gross_revenue_new_customers
  from
    `planar-depth-242012.uploads.backend_revenue`

  union all

  select
    'steuererklaerung-student.de' as account_name,
    date as date_day,
    sst_gross_revenue_old_customers as gross_revenue_old_customers,
    sst_gross_revenue_new_customers as gross_revenue_new_customers
  from
    `planar-depth-242012.uploads.backend_revenue`

  union all

  select
    'steuererklaerung-polizei.de' as account_name,
    date as date_day,
    spo_gross_revenue_old_customers as gross_revenue_old_customers,
    spo_gross_revenue_new_customers as gross_revenue_new_customers
  from
    `planar-depth-242012.uploads.backend_revenue`
  )

select
  account_name,
  date_day,
  cast (replace (replace (gross_revenue_old_customers, ".", ""), ",", ".") as numeric) / 1.19 as revenue_old_customers,
  cast (replace (replace (gross_revenue_new_customers, ".", ""), ",", ".") as numeric) / 1.19 as revenue_new_customers

from
  revenue_customers

order by
  date_day DESC
