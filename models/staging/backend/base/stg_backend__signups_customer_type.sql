with
  signups_customers as (
  select
    'lohnsteuer-kompakt.de' as account_name,
    date,
    lk_signups as signups
  from
    `planar-depth-242012.uploads.backend_signups`
  union all
  select
    'steuergo.de' as account_name,
    date,
    sg_signups as signups
  from
    `planar-depth-242012.uploads.backend_signups`
  union all
  select
    'steuererklaerung-student.de' as account_name,
    date,
    sst_signups as signups
  from
    `planar-depth-242012.uploads.backend_signups`
  union all
  select
    'steuererklaerung-polizei.de' as account_name,
    date,
    spo_signups as signups
  from
    `planar-depth-242012.uploads.backend_signups`)
select
  account_name,
  date,
  cast (replace (signups, ".", "") as numeric) as signups
from
  signups_customers
order by
  date DESC
