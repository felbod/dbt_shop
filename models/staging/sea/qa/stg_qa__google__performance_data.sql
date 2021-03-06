
/*
  - Checks base models for diverging performance indicators between different data sources.
  */

(select
    *
  , 'Google Ads' as platform
  , 'campaign_performance' as table

    from {{ref('stg_google_ads__campaigns__performance')}}

except distinct

select
    *
  , 'Google Ads' as platform
  , 'campaign_performance' as table

    from {{ref('stg_google_ads_1__campaigns__performance')}}

order by
  date_day desc
  , account_id
  , campaign_id)

union all

(select
    *
  , 'Google Ads' as platform
  , 'campaign_performance_1' as table

    from {{ref('stg_google_ads_1__campaigns__performance')}}

except distinct

select
    *
  , 'Google Ads' as platform
  , 'campaign_performance_1' as table

    from {{ref('stg_google_ads__campaigns__performance')}}

order by
  date_day desc
  , account_id
  , campaign_id)
