WITH
  starts_customers AS (
  SELECT
    'lohnsteuer-kompakt.de' AS account_name,
    date,
    lk_starts_old_customers AS starts_old_customers,
    lk_starts_new_customers AS starts_new_customers
  FROM
    `planar-depth-242012.uploads.backend_starts`
  UNION ALL
  SELECT
    'steuergo.de' AS account_name,
    date,
    sg_starts_old_customers AS starts_old_customers,
    sg_starts_new_customers AS starts_new_customers
  FROM
    `planar-depth-242012.uploads.backend_starts`
  UNION ALL
  SELECT
    'steuererklaerung-student.de' AS account_name,
    date,
    sst_starts_old_customers AS starts_old_customers,
    sst_starts_new_customers AS starts_new_customers
  FROM
    `planar-depth-242012.uploads.backend_starts`
  UNION ALL
  SELECT
    'steuererklaerung-polizei.de' AS account_name,
    date,
    spo_starts_old_customers AS starts_old_customers,
    spo_starts_new_customers AS starts_new_customers
  FROM
    `planar-depth-242012.uploads.backend_starts`)
SELECT
  account_name,
  date,
  CAST (replace (starts_old_customers, ".", "") AS numeric) AS starts_old_customers,
  CAST (replace (starts_new_customers, ".", "") AS numeric) AS starts_new_customers
FROM
  starts_customers
ORDER BY
  date DESC
