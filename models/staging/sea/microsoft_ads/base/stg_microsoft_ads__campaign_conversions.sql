
select *

from (

  select
    accountid as account_id
    , campaignid as campaign_id
    , cast(timeperiod as date) as date_day
    , goal as conversion_name
/* ab 24.03.20 da Stitch allconversions als STRING speichert */
    , cast(allconversions as float64) as conversions
    , cast(allrevenue as float64) as conversion_value_eur
    , if
      (FORMAT_DATE("%g", cast(timeperiod as date)) <> REGEXP_EXTRACT(goal, r"[0-9][0-9]$"),
        0,
        1) as is_new_customers
    , if
      (goal = "Signup",
        cast(allconversions as float64),
        0) as signups
    , if
      (STARTS_WITH(goal, "Start R"),
        cast(allconversions as float64),
        0) as starts
    , if
      (STARTS_WITH(goal, "Sale R"),
        cast(allconversions as float64),
        0) as sales

/* bis 24.03.20
    , allconversions as conversions
    , allrevenue as conversion_value_eur
    , if
      (FORMAT_DATE("%g", cast(timeperiod as date)) <> REGEXP_EXTRACT(goal, r"[0-9][0-9]$"),
        0,
        1) as is_new_customers
    , if
      (goal = "Signup",
        allconversions,
        0) as signups
    , if
      (STARTS_WITH(goal, "Start R"),
        allconversions,
        0) as starts
    , if
      (STARTS_WITH(goal, "Sale R"),
        allconversions,
        0) as sales
*/

    , RANK() OVER (PARTITION BY timeperiod, campaignid ORDER BY _sdc_report_datetime DESC) as rank -- siehe https://www.stitchdata.com/docs/integrations/saas/bing-ads

  from
    `planar-depth-242012.microsoft_ads__stitch.goals_and_funnels_report`

  order by
    timeperiod ASC ) AS latest

where
latest.rank = 1
