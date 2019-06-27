SELECT
  ExternalCustomerId AS account_id,
  AccountDescriptiveName AS account_name,
  AccountCurrencyCode AS account_currency_code
FROM
  `planar-depth-242012.google_ads.Customer_8644635112`
WHERE
  _DATA_DATE = _LATEST_DATE
ORDER BY
  ExternalCustomerId DESC
