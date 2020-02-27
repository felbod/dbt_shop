
with

  landingpages__analytics as (

    select *

    from {{ref('stg_google_analytics__landingpages')}}

  ),

  landingpages__search_console as (

    select *

    from {{ref('stg_google_search_console__pages')}}

  )

select
  landingpages__analytics.date_day,
  landingpages__analytics.landingpage_path,
  landingpages__analytics.device_category,
  landingpages__analytics.controller_id,
  landingpages__analytics.source_medium,
  landingpages__search_console.impressions,
  landingpages__search_console.clicks,
  landingpages__analytics.entrances,
  landingpages__analytics.signups,
  landingpages__analytics.sales

from landingpages__analytics
  left join landingpages__search_console on landingpages__analytics.date_day = landingpages__search_console.date_day
    and landingpages__analytics.landingpage_path = landingpages__search_console.landingpage_path
    and landingpages__analytics.device_category = landingpages__search_console.device_category
    and landingpages__analytics.controller_id = landingpages__search_console.controller_id
