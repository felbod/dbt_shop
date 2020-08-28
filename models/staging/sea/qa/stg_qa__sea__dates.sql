
/*
  - Checks all base models for NONE values to find dates missing in original data sets.
  */


  with campaign_conversions as (
    select
        date_day, account_id
      , 'Google Ads' as platform
      , 'campaign_performance' as table
        from {{ref('stg_google_ads__campaign_performance')}}
    union all
    select
        date_day, account_id
      , 'Google Ads' as platform
      , 'campaign_conversions' as table
        from {{ref('stg_google_ads__campaign_conversions')}}
    union all
    select
        date_day, account_id
      , 'Microsoft Ads' as platform
      , 'campaign_conversions' as table
        from {{ref('stg_microsoft_ads__campaign_performance')}}
    union all
    select
        date_day, account_id
      , 'Microsoft Ads' as platform
      , 'campaign_conversions' as table
        from {{ref('stg_microsoft_ads__campaign_conversions')}}
      )

  select *

  from campaign_conversions

  where account_id is NULL

  order by
      platform
    , table
    , date_day desc
    , account_id
