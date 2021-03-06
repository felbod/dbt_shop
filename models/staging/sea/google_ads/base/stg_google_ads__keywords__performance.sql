
with
  keyword_performance_usd as (
    select
        ExternalCustomerId as account_id
      , CampaignId as campaign_id
      , AdgroupId as adgroup_id
      , CriterionId as keyword_id
      , Date as date_day
      , sum (Impressions) as impressions
      , sum (Clicks) as clicks
      , sum (Conversions) as conversions
      , sum (ConversionValue) as conversion_value_usd
      , sum (Cost / 1000000) as cost_usd
    from
      `planar-depth-242012.google_ads__transfer.p_KeywordBasicStats_8644635112`
    group by
        date_day
      , account_id
      , campaign_id
      , adgroup_id
      , CriterionId
    )

  , exchange_rates as (
    select * from {{ref('stg_external__exchange_rates__all_dates')}})

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
  keyword_performance_usd.account_id
  , keyword_performance_usd.campaign_id
  , keyword_performance_usd.adgroup_id
  , keyword_performance_usd.keyword_id
  , dates.date_day
  , keyword_performance_usd.impressions
  , keyword_performance_usd.clicks
  , keyword_performance_usd.conversions
  , keyword_performance_usd.conversion_value_usd / exchange_rates.exchange_rate_eur_usd as conversion_value_eur
  , keyword_performance_usd.cost_usd
  , keyword_performance_usd.cost_usd / exchange_rates.exchange_rate_eur_usd as cost_eur
  , safe_divide(
    keyword_performance_usd.cost_usd,
    keyword_performance_usd.clicks)
    / exchange_rates.exchange_rate_eur_usd
    as cost_per_click_eur

from
  keyword_performance_usd
  left join exchange_rates on exchange_rates.date_day = keyword_performance_usd.date_day
  full join dates on dates.date_day = keyword_performance_usd.date_day
