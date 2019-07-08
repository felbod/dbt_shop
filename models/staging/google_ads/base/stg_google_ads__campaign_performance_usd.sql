
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
