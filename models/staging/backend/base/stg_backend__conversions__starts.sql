
select
  cast(date_day as date) as date_day,
  controller_id,
  starts_old_customers as starts__old_customers,
  starts_new_customers as starts__new_customers

from `planar-depth-242012.backend_data.backend__conversions__starts`
