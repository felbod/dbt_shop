
# Original example
# https://stackoverflow.com/questions/53319155/filling-missing-dates-in-bigquery-sql-without-creating-a-new-calendar/53323243#53323243

with exchange_rates as (

  select *

    from {{ref('stg_external__exchange_rates')}}

),

 days as (

  select day

  from (

    select
      min (date (timestamp (date_day))) min_date,
      max (date (timestamp (current_date()))) today

    from exchange_rates

  ), unnest (generate_date_array (min_date, today)) day

)
select
  day as date_day,
  last_value (exchange_rate_eur_usd ignore nulls) over (order by day) exchange_rate_eur_usd

from days

left join exchange_rates
on day = DATE(TIMESTAMP(date_day))

order by day
