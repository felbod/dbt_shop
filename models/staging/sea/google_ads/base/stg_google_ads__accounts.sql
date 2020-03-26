
select
  'Google Ads' as platform_name,
  ExternalCustomerId as account_id,
  AccountDescriptiveName as account_name,
  AccountCurrencyCode as account_currency_code,
  case
    when AccountDescriptiveName like '%lohnsteuer-kompakt.de%' then 'lohnsteuer-kompakt.de'
    when AccountDescriptiveName like '%steuergo.de%' then 'steuergo.de'
    when AccountDescriptiveName like '%steuererklaerung-student.de%' then 'steuererklaerung-student.de'
    when AccountDescriptiveName like '%steuererklaerung-polizei.de%' then 'steuererklaerung-polizei.de'
  end as brand_name

from
  `planar-depth-242012.google_ads__bigquery_transfer.Customer_8644635112`

where
  _DATA_DATE = _LATEST_DATE

order by
  ExternalCustomerId desc
