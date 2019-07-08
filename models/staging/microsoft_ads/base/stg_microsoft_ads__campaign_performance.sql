
select
  accountid as account_id,
  campaignid as campaign_id,
  cast(timeperiod as date) as date_day,
  impressions as impressions,
  clicks as clicks,
  conversions as conversions,
  revenue as conversion_value_eur,
  spend as cost_eur,
  safe_divide (spend, clicks) as cost_per_click_eur

from
  `planar-depth-242012.microsoft_ads_stitch.campaign_performance_report`
