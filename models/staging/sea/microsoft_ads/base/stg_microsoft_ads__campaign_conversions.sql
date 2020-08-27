
with
  campaign_conversions as (
    select *
    from (
      select
        accountid as account_id
        , campaignid as campaign_id
        , cast(timeperiod as date) as date_day
        , goal as conversion_name
        , cast(allconversions as float64) as conversions /* ab 24.03.20 da Stitch 'allconversions' als STRING speichert */
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
        , RANK() OVER (PARTITION BY timeperiod, campaignid ORDER BY _sdc_report_datetime DESC) as rank -- siehe https://www.stitchdata.com/docs/integrations/saas/bing-ads
      from
        `planar-depth-242012.microsoft_ads__stitch.goals_and_funnels_report`
      order by
        timeperiod ASC ) AS latest
    where
      latest.rank = 1)

    /*
      - For quality assurance
      - To find dates without values
      - These will appear as NONE in the result
      */
  , dates as(
    select date_day
      from unnest(
          generate_date_array(date('2018-01-01'), current_date(), interval 1 day)
      ) as date_day)

select
  campaign_conversions.account_id
  , campaign_conversions.campaign_id
  , dates.date_day
  , campaign_conversions.conversion_name
  , campaign_conversions.conversions
  , campaign_conversions.conversion_value_eur
  , campaign_conversions.is_new_customers
  , campaign_conversions.signups
  , campaign_conversions.starts
  , campaign_conversions.sales
  , campaign_conversions.rank

from
  campaign_conversions
  full join dates on dates.date_day = campaign_conversions.date_day
