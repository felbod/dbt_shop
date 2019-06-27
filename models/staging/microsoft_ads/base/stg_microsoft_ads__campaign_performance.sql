SELECT
  accountid AS account_id,
  campaignid AS campaign_id,
  cast(timeperiod as date) AS date,
  impressions AS impressions,
  clicks AS clicks,
  conversions AS conversions,
  revenue AS conversion_value_eur,
  spend AS cost_eur
FROM
  `planar-depth-242012.microsoft_ads_stitch.campaign_performance_report`
