
select
  account_id,
  campaign_id,
  date_day,
  sum (Impressions) as impressions,
  sum (Clicks) as clicks,
  sum (Conversions) as conversions,
  sum (conversion_value_usd) as conversion_value_usd,
  sum (cost_usd) as cost_usd,
  safe_divide ( sum (cost_usd),  sum (Clicks) ) as cost_per_click_usd

from (

select
  ExternalCustomerId as account_id,
  CampaignId as campaign_id,
  Date as date_day,
  Impressions as impressions,
  Clicks as clicks,
  Conversions as conversions,
  ConversionValue as conversion_value_usd,
  Cost / 1000000 as cost_usd

from
  `planar-depth-242012.google_ads.p_CampaignBasicStats_8644635112`

)

group by
  date_day,
  account_id,
  campaign_id
