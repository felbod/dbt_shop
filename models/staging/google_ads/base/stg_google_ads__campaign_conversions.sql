
with campaign_conversions_usd as (

  select *

    from {{ref('stg_google_ads__campaign_conversions_usd')}}

),

exchange_rates as (

  select *

    from {{ref('stg_external__exchange_rates_all_dates')}}

)

select
  campaign_conversions_usd.*,
  campaign_conversions_usd.conversion_value_usd / exchange_rates.exchange_rate_eur_usd as conversion_value_eur

from
  campaign_conversions_usd
  left join exchange_rates on exchange_rates.day = campaign_conversions_usd.date_day
