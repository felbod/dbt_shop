/*
SELECT
  platform_name,
  account_id,
  campaign_id,
  account_name,
  campaign_name,
  CASE
    WHEN @PERIOD = 'YEAR' THEN DATE_TRUNC(date_day, YEAR)
    WHEN @PERIOD = 'QUARTER' THEN DATE_TRUNC(date_day, QUARTER)
    WHEN @PERIOD = 'MONTH' THEN DATE_TRUNC(date_day, MONTH)
    WHEN @PERIOD = 'ISOWEEK' THEN DATE_TRUNC(date_day, ISOWEEK)
    WHEN @PERIOD = 'DAY' THEN date_day
    END
  AS period,
  SUM (impressions) AS impressions,
  SUM (clicks) AS clicks,
  SUM (clicks_new_customers) AS clicks_new_customers,
  SUM (clicks_old_customers) AS clicks_old_customers,
  SUM (conversions) AS conversions,
  SUM (conversion_value_eur) AS conversion_value_eur,
  SUM (conversion_value_eur_new_customers) AS conversion_value_eur_new_customers,
  SUM (conversion_value_eur_old_customers) AS conversion_value_eur_old_customers,
  SUM (cost_eur) AS cost_eur,
  SUM (cost_eur_old_customers) AS cost_eur_old_customers,
  SUM (cost_eur_new_customers) AS cost_eur_new_customers,
  SUM (signups) AS signups,
  SUM (starts) AS starts,
  SUM (starts_new_customers) AS starts_new_customers,
  SUM (starts_old_customers) AS starts_old_customers,
  SUM (sales) AS sales,
  SUM (sales_new_customers) AS sales_new_customers,
  SUM (sales_old_customers) AS sales_old_customers

FROM
  `planar-depth-242012.dbt_dev.mrts_combined__campaigns_customer_type`

WHERE
  date_day >= @DATE_MIN AND
  date_day <= @DATE_MAX

GROUP BY
  period,
  platform_name,
  account_id,
  campaign_id,
  account_name,
  campaign_name

*/
