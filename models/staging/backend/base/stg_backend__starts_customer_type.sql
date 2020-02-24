
-- ersetzen durch neue Backend-Tabellen

with

  starts_customers as (

  select
    'lohnsteuer-kompakt.de' as account_name,
    date,
    lk_starts_old_customers as starts_old_customers,
    lk_starts_new_customers as starts_new_customers
  from
    `planar-depth-242012.uploads.backend_starts`

  union all

  select
    'steuergo.de' as account_name,
    date,
    sg_starts_old_customers as starts_old_customers,
    sg_starts_new_customers as starts_new_customers
  from
    `planar-depth-242012.uploads.backend_starts`

  union all

  select
    'steuererklaerung-student.de' as account_name,
    date,
    sst_starts_old_customers as starts_old_customers,
    sst_starts_new_customers as starts_new_customers
  from
    `planar-depth-242012.uploads.backend_starts`

  union all

  select
    'steuererklaerung-polizei.de' as account_name,
    date,
    spo_starts_old_customers as starts_old_customers,
    spo_starts_new_customers as starts_new_customers
  from
    `planar-depth-242012.uploads.backend_starts`

  )

select
  account_name,
  date as date_day,
  cast (replace (starts_old_customers, ".", "") as numeric) as starts_old_customers,
  cast (replace (starts_new_customers, ".", "") as numeric) as starts_new_customers

from
  starts_customers

order by
  date_day desc
