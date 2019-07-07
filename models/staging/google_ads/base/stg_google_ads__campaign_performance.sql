
with campaign_performance_usd as (

  select *

    from {{ref('stg_google_ads__campaign_performance_usd')}}

),

exchange_rates as (

  select *

    from {{ref('stg_external__exchange_rates_all_dates')}}

)

select
  campaign_performance_usd.*,
  campaign_performance_usd.conversion_value_usd / exchange_rates.exchange_rate_eur_usd as conversion_value_eur,
  campaign_performance_usd.cost_usd / exchange_rates.exchange_rate_eur_usd as cost_eur

from
  campaign_performance_usd
  left join exchange_rates on exchange_rates.day = campaign_performance_usd.date_day
