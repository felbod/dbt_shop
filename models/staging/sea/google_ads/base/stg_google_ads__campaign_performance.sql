
with campaign_performance_usd as (

  select
    ExternalCustomerId as account_id,
    CampaignId as campaign_id,
    Date as date_day,
    sum (Impressions) as impressions,
    sum (Clicks) as clicks,
    sum (Conversions) as conversions,
    sum (ConversionValue) as conversion_value_usd,
    sum (Cost / 1000000) as cost_usd

  from
    `planar-depth-242012.google_ads.p_CampaignBasicStats_8644635112`

  group by
    date_day,
    account_id,
    campaign_id

),

exchange_rates as (

  select *

    from {{ref('stg_external__exchange_rates_all_dates')}}

)

select
  account_id,
  campaign_id,
  campaign_performance_usd.date_day,
  impressions,
  clicks,
  conversions,
  conversion_value_usd / exchange_rate_eur_usd as conversion_value_eur,
  cost_usd / exchange_rate_eur_usd as cost_eur,
  safe_divide (cost_usd, clicks) / exchange_rate_eur_usd as cost_per_click_eur

from
  campaign_performance_usd
  left join exchange_rates on exchange_rates.date_day = campaign_performance_usd.date_day
