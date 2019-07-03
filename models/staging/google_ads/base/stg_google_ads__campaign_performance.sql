select
  ExternalCustomerId as account_id,
  CampaignId as campaign_id,
  Date as date,
  SUM(Impressions) as impressions,
  SUM(Clicks) as clicks,
  SUM(Conversions) as conversions,
  SUM(ConversionValue) as conversion_value_usd,
  SUM(Cost)/1000000 as cost_usd
from
  `planar-depth-242012.google_ads.p_CampaignBasicStats_8644635112`
GROUP BY
  Date,
  ExternalCustomerId,
  CampaignId
