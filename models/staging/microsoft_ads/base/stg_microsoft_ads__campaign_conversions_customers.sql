
with campaign_conversions as (

  select *

    from {{ref('stg_microsoft_ads__campaign_conversions')}}

)

SELECT
  date,
  account_id,
  campaign_id,
  SUM(signups) AS signups,
  SUM(is_new_customers*conversion_value_eur) AS conversion_value_eur_new_customers,
  SUM((is_new_customers-1)*(-1)*conversion_value_eur) AS conversion_value_eur_old_customers,
  SUM(is_new_customers*starts) AS starts_new_customers,
  SUM((is_new_customers-1)*(-1)*starts) AS starts_old_customers,
  SUM(is_new_customers*Sales) AS sales_new_customers,
  SUM((is_new_customers-1)*(-1)*Sales) AS sales_old_customers

from campaign_conversions

GROUP BY
  date,
  account_id,
  campaign_id
