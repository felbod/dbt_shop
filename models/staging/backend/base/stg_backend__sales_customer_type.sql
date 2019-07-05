with
  sales_customers as (
  select
    'lohnsteuer-kompakt.de' as account_name,
    date,
    lk_sales_old_customers as sales_old_customers,
    lk_sales_new_customers as sales_new_customers
  from
    `planar-depth-242012.uploads.backend_sales`
  union all
  select
    'steuergo.de' as account_name,
    date,
    sg_sales_old_customers as sales_old_customers,
    sg_sales_new_customers as sales_new_customers
  from
    `planar-depth-242012.uploads.backend_sales`
  union all
  select
    'steuererklaerung-student.de' as account_name,
    date,
    sst_sales_old_customers as sales_old_customers,
    sst_sales_new_customers as sales_new_customers
  from
    `planar-depth-242012.uploads.backend_sales`
  union all
  select
    'steuererklaerung-polizei.de' as account_name,
    date,
    spo_sales_old_customers as sales_old_customers,
    spo_sales_new_customers as sales_new_customers
  from
    `planar-depth-242012.uploads.backend_sales`)
select
  account_name,
  date,
  cast (replace (sales_old_customers, ".", "") as numeric) as sales_old_customers,
  cast (replace (sales_new_customers, ".", "") as numeric) as sales_new_customers
from
  sales_customers
order by
  date DESC
