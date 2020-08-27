
select *

from (

  select
    cast(date_day as date) as date_day
    , report_datetime
    , controller_id
    , sales_old_customers as sales__old_customers
    , sales_new_customers as sales__new_customers

    /* Revenues from backend:
    - Reported as gross values in Euro
    - Updated every morning @ 6:05 a.m. UTC (08:05 Berlin time)
    - Includes values for previous 30 (?) days */
    , revenue_old_customers / 1.19 as revenue_net_eur__old_customers
    , revenue_new_customers / 1.19 as revenue_net_eur__new_customers

    , rank() over (partition by date_day, controller_id order by report_datetime desc) as rank
    -- analog zu Microsoft-Conversion bei Stitch
    -- siehe https://www.stitchdata.com/docs/integrations/saas/bing-ads


  from `planar-depth-242012.backend_data.backend__conversions__sales_revenue`

  order by
    date_day desc
    , controller_id

  ) as latest

where latest.rank = 1
