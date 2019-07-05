
with exchange_rates as (

  select *

    from {{ref('stg_external__exchange_rates')}}

),

days as (

  select day

  from (

    select
      min(date(timestamp(date))) min_dt,
      max(date(timestamp(date))) max_dt

    from exchange_rates

  ),

    unnest(generate_date_array(min_dt, max_dt)) day

)

select
  day,
  last_value(exchange_rate_eur_usd ignore nulls) over(order by day) exchange_rate_eur_usd

from days

left join exchange_rates
  on day = date(timestamp(date))

order by
  day desc
