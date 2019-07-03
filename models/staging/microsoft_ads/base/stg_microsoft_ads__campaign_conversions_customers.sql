
with campaign_conversions as (

  select *

    from {{ref('stg_microsoft_ads__campaign_conversions')}}

)

select
  date,
  account_id,
  campaign_id,
  SUM(signups) as signups,
  SUM(is_new_customers*conversion_value_eur) as conversion_value_eur_new_customers,
  SUM((is_new_customers-1)*(-1)*conversion_value_eur) as conversion_value_eur_old_customers,
  SUM(is_new_customers*starts) as starts_new_customers,
  SUM((is_new_customers-1)*(-1)*starts) as starts_old_customers,
  SUM(is_new_customers*Sales) as sales_new_customers,
  SUM((is_new_customers-1)*(-1)*Sales) as sales_old_customers

from campaign_conversions

GROUP BY
  date,
  account_id,
  campaign_id
