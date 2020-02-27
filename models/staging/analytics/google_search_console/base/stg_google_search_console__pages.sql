
select
  cast(date_day as date) as date_day,
  controller_id,
  -- query,
  page as landingpage_path,
  lower(device) as device_category,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  -- ctr,
  'to do: weighted position' as position

from
  `planar-depth-242012.google_search_console__api.search_console__pages`

group by
  date_day,
  controller_id,
  page,
  device
