
select
  cast(date_day as date) as date_day,
  controller_id,
  signups_new_customers as signups__new_customers

from `planar-depth-242012.backend_data.backend__conversions__signups`
