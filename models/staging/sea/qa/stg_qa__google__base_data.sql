
/*
  - Checks base models for diverging base data (account_id, campaign_id, date_day) between different data sources.
  */

(select
    account_id
  , campaign_id
  , date_day
  , 'Google Ads' as platform
  , 'campaign_performance' as table

    from {{ref('stg_google_ads__campaign_performance')}}

except distinct

select
    account_id
  , campaign_id
  , date_day
  , 'Google Ads' as platform
  , 'campaign_performance' as table

    from {{ref('stg_google_ads_1__campaign_performance')}}

order by
  date_day desc
  , account_id
  , campaign_id)

union all

(select
    account_id
  , campaign_id
  , date_day
  , 'Google Ads' as platform
  , 'campaign_performance_1' as table

    from {{ref('stg_google_ads_1__campaign_performance')}}

except distinct

select
    account_id
  , campaign_id
  , date_day
  , 'Google Ads' as platform
  , 'campaign_performance_1' as table

    from {{ref('stg_google_ads__campaign_performance')}}

order by
  date_day desc
  , account_id
  , campaign_id)
