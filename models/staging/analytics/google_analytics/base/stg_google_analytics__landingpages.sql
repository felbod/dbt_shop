
select
  cast(start_date as date) as date_day,
  -- start_date as date_day, -- automatisch in Stitch-Daten enthalten
  -- date     -- nicht benötigt, identisch mit start_date; kann in Stitch integration entfernt werden
  -- end_date -- nicht benötigt, identisch mit start_date; automatisch in Stitch-Daten enthalten
  landingpagepath as landingpage_path,
  sourcemedium as source_medium,
  devicecategory as device_category,
  4 as controller_id,
  entrances,
  goal3completions as signups,
  transactions as sales

  from
    `planar-depth-242012.google_analytics_stitch__landingpages__4.report`

  where
    sourcemedium = 'google / organic'

union all

select
  cast(start_date as date) as date_day,
  landingpagepath as landingpage_path,
  sourcemedium as source_medium,
  devicecategory as device_category,
  5 as controller_id,
  entrances,
  goal1completions as signups,
  transactions as sales

  from
    `planar-depth-242012.google_analytics_stitch__landingpages__5.report`

  where
    sourcemedium = 'google / organic'

union all

select
  cast(start_date as date) as date_day,
  landingpagepath as landingpage_path,
  sourcemedium as source_medium,
  devicecategory as device_category,
  501 as controller_id,
  entrances,
  goal1completions as signups,
  transactions as sales

  from
    `planar-depth-242012.google_analytics_stitch__landingpages__501.report`

  where
    sourcemedium = 'google / organic'

union all

select
  cast(start_date as date) as date_day,
  landingpagepath as landingpage_path,
  sourcemedium as source_medium,
  devicecategory as device_category,
  504 as controller_id,
  entrances,
  goal1completions as signups,
  transactions as sales

  from
    `planar-depth-242012.google_analytics_stitch__landingpages__504.report`

  where
    sourcemedium = 'google / organic'
