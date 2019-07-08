
select
  Datum as date_day,
  Rate as exchange_rate_eur_usd

from
  `planar-depth-242012.external_data.exchange_rate_eur_usd`

order by
  Datum desc
