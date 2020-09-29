
with
  campaign_performance_usd as (
    select
        ExternalCustomerId as account_id
      , CampaignId as campaign_id
      , AdNetworkType1 as ad_network_type_1
      , AdNetworkType2 as ad_network_type_2
      , Date as date_day
      , sum (Impressions) as impressions
      , sum (Clicks) as clicks
      , sum (Conversions) as conversions
      , sum (ConversionValue) as conversion_value_usd
      , sum (Cost / 1000000) as cost_usd
    from
      `planar-depth-242012.google_ads__transfer.p_CampaignBasicStats_8644635112`
    group by
      date_day
      , account_id
      , campaign_id
      , ad_network_type_1
      , ad_network_type_2
    )

  , campaign_cross_device as (
    select
        ExternalCustomerId as account_id
      , CampaignId as campaign_id
      , AdNetworkType1 as ad_network_type_1
      , AdNetworkType2 as ad_network_type_2
      , Date as date_day
      , (cast(replace(replace(SearchImpressionShare, "< 10%", "5.00%"), "%", "") as float64) / 100)
        as search_impression_share
    from
      `planar-depth-242012.google_ads__transfer.p_CampaignCrossDeviceStats_8644635112`
    )

  , exchange_rates as (
    select * from {{ref('stg_external__exchange_rates_all_dates')}}
    )

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
      ) as date_day
    )

select
  campaign_performance_usd.account_id
  , campaign_performance_usd.campaign_id
  , dates.date_day
--  , campaign_performance_usd.ad_network_type_1
--  , campaign_performance_usd.ad_network_type_2

  , sum(campaign_performance_usd.impressions) as impressions
  , sum(campaign_performance_usd.clicks) as clicks
  , sum(campaign_performance_usd.conversions) as conversions
  , sum(campaign_performance_usd.conversion_value_usd / exchange_rates.exchange_rate_eur_usd)
    as conversion_value_eur
  , sum(campaign_performance_usd.cost_usd / exchange_rates.exchange_rate_eur_usd) as cost_eur
  , sum(campaign_cross_device.search_impression_share) as search_impression_share
  , sum(
      safe_divide(campaign_performance_usd.impressions, campaign_cross_device.search_impression_share))
      as search_eligible_impressions
  , sum(campaign_performance_usd.cost_usd) as cost_usd  -- only with Google Ads
/* -- cpc calculated in subsequent query
  , safe_divide(
      sum(campaign_performance_usd.cost_usd), sum(campaign_performance_usd.clicks)) / avg(exchange_rates.exchange_rate_eur_usd)
      as cost_per_click_eur
*/

from
  campaign_performance_usd
  left join exchange_rates
    on campaign_performance_usd.date_day = exchange_rates.date_day
  left join campaign_cross_device
    on campaign_performance_usd.date_day = campaign_cross_device.date_day
    and campaign_performance_usd.campaign_id = campaign_cross_device.campaign_id
    and campaign_performance_usd.ad_network_type_1 = campaign_cross_device.ad_network_type_1
    and campaign_performance_usd.ad_network_type_2 = campaign_cross_device.ad_network_type_2
  full join dates
    on dates.date_day = campaign_performance_usd.date_day

group by
  campaign_performance_usd.account_id
  , campaign_performance_usd.campaign_id
  , dates.date_day
--  , campaign_performance_usd.ad_network_type_1
--  , campaign_performance_usd.ad_network_type_2
