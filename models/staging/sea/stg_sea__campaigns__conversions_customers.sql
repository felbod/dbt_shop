
with campaign_conversions as (
  select *, 'Google Ads' as platform
    from {{ref('stg_google_ads__campaigns__conversions')}}
  union all
  select * except (rank), 'Microsoft Ads' as platform
    from {{ref('stg_microsoft_ads__campaigns__conversions')}})

select
  date_day
  , platform
  , account_id
  , campaign_id

  , sum (signups) as signups
  , sum (conversion_value_eur) as conversion_value_eur
  , sum (is_new_customers * conversion_value_eur)
    as conversion_value_eur_new_customers
  , sum (if (is_new_customers = 1, 0, conversion_value_eur))
    as conversion_value_eur_old_customers
  , sum (if (is_new_customers = 1, 0, conversions))
    as clicks_old_customers -- Assumption: 1 click per conversion by old customers
  , sum (starts) as starts
  , sum (is_new_customers * starts) as starts__new_customers
  , sum (if (is_new_customers = 1, 0, starts))
    as starts__old_customers
  , sum (sales) as sales
  , sum (is_new_customers * sales) as sales__new_customers
  , sum (if (is_new_customers = 1, 0, sales))
    as sales__old_customers

from campaign_conversions

group by
  date_day
  , platform
  , account_id
  , campaign_id
