
select
  Datum as date,
  Rate as exchange_rate_eur_usd
  /*,
  avg(Rate) over (order by unix_date(Datum) range between 27 preceding and current row) as exchange_rate_eur_usd_mov_28d
  */

from
  `planar-depth-242012.external_data.exchange_rate_eur_usd`

order by
  Datum desc
