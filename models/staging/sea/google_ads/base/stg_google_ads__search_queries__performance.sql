
with
  search_query_performance_usd as (
    select
        ExternalCustomerId as account_id
      , CampaignId as campaign_id
      , AdGroupId as adgroup_id
      , CriterionId as keyword_id
      , QueryMatchTypeWithVariant as match_type
      , Query as search_query
--      , AdNetworkType1 as ad_network_type_1
--      , AdNetworkType2 as ad_network_type_2
      , Date as date_day
      , sum (Impressions) as impressions
--      , if (AdNetworkType1 = "SEARCH", sum(Impressions), 0) as search_impressions
      , sum (Clicks) as clicks
      , sum (Conversions) as conversions
      , sum (ConversionValue) as conversion_value_usd
      , sum (Cost / 1000000) as cost_usd
    from
      `planar-depth-242012.google_ads__transfer.p_SearchQueryStats_8644635112`
    group by
      date_day
      , account_id
      , campaign_id
      , adgroup_id
      , keyword_id
      , match_type
      , search_query
--      , ad_network_type_1
--      , ad_network_type_2
    )

/*
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
*/

  , exchange_rates as (
    select * from {{ref('stg_external__exchange_rates__all_dates')}}
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
  search_query_performance_usd.account_id
  , search_query_performance_usd.campaign_id
  , search_query_performance_usd.adgroup_id
  , search_query_performance_usd.keyword_id
  , search_query_performance_usd.match_type
  , search_query_performance_usd.search_query
  , dates.date_day
--  , search_query_performance_usd.ad_network_type_1
--  , search_query_performance_usd.ad_network_type_2
  , sum(search_query_performance_usd.impressions) as impressions
--  , sum(search_query_performance_usd.search_impressions) as search_impressions
  , sum(search_query_performance_usd.clicks) as clicks
  , sum(search_query_performance_usd.conversions) as conversions
  , sum(search_query_performance_usd.conversion_value_usd / exchange_rates.exchange_rate_eur_usd) as conversion_value_eur
  , sum(search_query_performance_usd.cost_usd / exchange_rates.exchange_rate_eur_usd) as cost_eur
--  , sum(campaign_cross_device.search_impression_share) as search_impression_share
--  , sum(safe_divide(search_query_performance_usd.impressions, campaign_cross_device.search_impression_share)) as search_eligible_impressions
  , sum(search_query_performance_usd.cost_usd) as cost_usd  -- only with Google Ads
/* -- cpc calculated in subsequent query
  , safe_divide(
      sum(search_query_performance_usd.cost_usd), sum(search_query_performance_usd.clicks)) / avg(exchange_rates.exchange_rate_eur_usd)
      as cost_per_click_eur
*/

from
  search_query_performance_usd
  left join exchange_rates
    on search_query_performance_usd.date_day = exchange_rates.date_day
/*
  left join campaign_cross_device
    on search_query_performance_usd.date_day = campaign_cross_device.date_day
    and search_query_performance_usd.campaign_id = campaign_cross_device.campaign_id
    and search_query_performance_usd.ad_network_type_1 = campaign_cross_device.ad_network_type_1
    and search_query_performance_usd.ad_network_type_2 = campaign_cross_device.ad_network_type_2
*/
  full join dates
    on dates.date_day = search_query_performance_usd.date_day

group by
  account_id
  , campaign_id
  , adgroup_id
  , keyword_id
  , match_type
  , search_query
  , date_day
--  , search_query_performance_usd.ad_network_type_1
--  , search_query_performance_usd.ad_network_type_2
