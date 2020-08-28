
with
  campaign_performance as (
    select *
    from (
      select
        accountid as account_id
        , campaignid as campaign_id
        , cast(timeperiod as date) as date_day
        , impressions as impressions
        , clicks as clicks
        , conversions as conversions
        , revenue as conversion_value_eur
        , spend as cost_eur
        , safe_divide(
          spend
          , clicks)
          as cost_per_click_eur
        , RANK() OVER (PARTITION BY timeperiod, campaignid ORDER BY _sdc_report_datetime DESC) as rank -- siehe https://www.stitchdata.com/docs/integrations/saas/bing-ads
      from
        `planar-depth-242012.microsoft_ads__stitch.campaign_performance_report`
      order by
        timeperiod ASC ) AS latest
    where
      latest.rank = 1)

      /*
        - For quality assurance
        - To find dates without values
        - These will appear as NONE in the result
        - Excluding today
        */
    , dates as (
      select date_day
        from unnest(
            generate_date_array (date('2018-01-01'), date_sub(current_date, interval 1 day), interval 1 day)
        ) as date_day)

select
  campaign_performance.account_id
  , campaign_performance.campaign_id
  , dates.date_day
  , campaign_performance.impressions
  , campaign_performance.clicks
  , campaign_performance.conversions
  , campaign_performance.conversion_value_eur
  , campaign_performance.cost_eur
  , campaign_performance.cost_per_click_eur
  , campaign_performance.rank

from
  campaign_performance
  full join dates on dates.date_day = campaign_performance.date_day
