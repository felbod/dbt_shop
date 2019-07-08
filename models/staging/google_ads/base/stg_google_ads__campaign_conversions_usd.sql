
select
  concat(cast (date as string), cast (CampaignId as string), ConversionTypeName) as id_me,
  ExternalCustomerId as account_id,
  CampaignId as campaign_id,
  Date as date_day,
  ConversionTypeName as conversion_name,
  sum(AllConversions) as conversions,
  sum(AllConversionValue) as conversion_value_usd,
  if (FORMAT_DATE("%g", Date) <> regexp_extract(ConversionTypeName, r"[0-9][0-9]$"),
    0,
    1) as is_new_customers,
  if (ConversionTypeName = "Signup",
    sum(AllConversions),
    0) as signups,
  if (STARTS_WITH(ConversionTypeName, "Start R"),
    sum(AllConversions),
    0) as starts,
  if (STARTS_WITH(ConversionTypeName, "Sale R"),
    sum(AllConversions),
    0) as sales

from
  `planar-depth-242012.google_ads.p_CampaignCrossDeviceConversionStats_8644635112`

group by
  ExternalCustomerId,
  CampaignId,
  Date,
  ConversionTypeName
