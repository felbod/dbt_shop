SELECT
  accountid AS account_id,
  campaignid AS campaign_id,
  cast(timeperiod as date) AS date,
  goal AS conversion_name,
  conversions AS conversions,
  revenue as conversion_value_eur,
IF
  (FORMAT_DATE("%g", cast(timeperiod as date)) <> REGEXP_EXTRACT(goal, r"[0-9][0-9]$"),
    0,
    1) AS is_new_customers,
IF
  (goal = "Signup",
    conversions,
    0) AS signups,
IF
  (STARTS_WITH(goal, "Start R"),
    conversions,
    0) AS starts,
IF
  (STARTS_WITH(goal, "Sale R"),
    conversions,
    0) AS sales
FROM
  `planar-depth-242012.microsoft_ads_stitch.goals_and_funnels_report`
