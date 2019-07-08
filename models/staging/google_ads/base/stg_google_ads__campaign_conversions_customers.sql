
with campaign_conversions as (

  select *

    from {{ref('stg_google_ads__campaign_conversions')}}

)

select
  date_day,
  account_id,
  campaign_id,
  sum (signups) as signups,
  sum (is_new_customers * conversion_value_eur) as conversion_value_eur_new_customers,
  sum ( (is_new_customers - 1) * (-1) * conversion_value_eur) as conversion_value_eur_old_customers,
  sum ( (is_new_customers - 1) * (-1) * conversions) as clicks_old_customers,
  sum (is_new_customers * starts) as starts_new_customers,
  sum ( (is_new_customers - 1) * (-1) * starts) as starts_old_customers,
  sum (is_new_customers * sales) as sales_new_customers, -- Fix? 'sales' with small 's'
--  sum(is_new_customers * Sales) as sales_new_customers,
  sum ( (is_new_customers - 1) * (-1) * sales) as sales_old_customers  -- Fix? 'sales' with small 's'
--  sum((is_new_customers-1) * (-1) * Sales) as sales_old_customers

from campaign_conversions

group by
  date_day,
  account_id,
  campaign_id
