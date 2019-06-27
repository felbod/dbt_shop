SELECT
  ExternalCustomerId AS account_id,
  CampaignId AS campaign_id,
  Date AS date,
  SUM(Impressions) AS impressions,
  SUM(Clicks) AS clicks,
  SUM(Conversions) AS conversions,
  SUM(ConversionValue) AS conversion_value_usd,
  SUM(Cost)/1000000 AS cost_usd
FROM
  `planar-depth-242012.google_ads.p_CampaignBasicStats_8644635112`
GROUP BY
  Date,
  ExternalCustomerId,
  CampaignId
