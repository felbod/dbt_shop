SELECT
  ExternalCustomerId as account_id,
  CampaignId as campaign_id,
  Date as date,
  ConversionTypeName as conversion_name,
  AllConversions as conversions,
  AllConversionValue as conversion_value_usd,
IF
  (FORMAT_DATE("%g", Date) <> REGEXP_EXTRACT(ConversionTypeName, r"[0-9][0-9]$"),
    0,
    1) AS is_new_customers,
IF
  (ConversionTypeName = "Signup",
    AllConversions,
    0) AS signups,
IF
  (STARTS_WITH(ConversionTypeName, "Start R"),
    AllConversions,
    0) AS starts,
IF
  (STARTS_WITH(ConversionTypeName, "Sale R"),
    AllConversions,
    0) AS sales
FROM
  `planar-depth-242012.google_ads.p_CampaignCrossDeviceConversionStats_8644635112`
