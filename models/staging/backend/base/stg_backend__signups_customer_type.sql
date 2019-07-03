WITH
  signups_customers AS (
  SELECT
    'lohnsteuer-kompakt.de' AS account_name,
    date,
    lk_signups AS signups
  FROM
    `planar-depth-242012.uploads.backend_signups`
  UNION ALL
  SELECT
    'steuergo.de' AS account_name,
    date,
    sg_signups AS signups
  FROM
    `planar-depth-242012.uploads.backend_signups`
  UNION ALL
  SELECT
    'steuererklaerung-student.de' AS account_name,
    date,
    sst_signups AS signups
  FROM
    `planar-depth-242012.uploads.backend_signups`
  UNION ALL
  SELECT
    'steuererklaerung-polizei.de' AS account_name,
    date,
    spo_signups AS signups
  FROM
    `planar-depth-242012.uploads.backend_signups`)
SELECT
  account_name,
  date,
  CAST (replace (signups, ".", "") AS numeric) AS signups
FROM
  signups_customers
ORDER BY
  date DESC
