select
  accountid as account_id,
  campaignid as campaign_id,
  cast(timeperiod as date) as date,
  goal as conversion_name,
  conversions as conversions,
  revenue as conversion_value_eur,
IF
  (FORMAT_DATE("%g", cast(timeperiod as date)) <> REGEXP_EXTRACT(goal, r"[0-9][0-9]$"),
    0,
    1) as is_new_customers,
IF
  (goal = "Signup",
    conversions,
    0) as signups,
IF
  (STARTS_WITH(goal, "Start R"),
    conversions,
    0) as starts,
IF
  (STARTS_WITH(goal, "Sale R"),
    conversions,
    0) as sales
from
  `planar-depth-242012.microsoft_ads_stitch.goals_and_funnels_report`
