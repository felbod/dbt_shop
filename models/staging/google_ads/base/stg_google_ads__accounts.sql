select
  ExternalCustomerId as account_id,
  AccountDescriptiveName as account_name,
  AccountCurrencyCode as account_currency_code
from
  `planar-depth-242012.google_ads.Customer_8644635112`
where
  _DATA_DATE = _LATEST_DATE
order by
  ExternalCustomerId desc
