WITH
  sales_customers AS (
  SELECT
    'lohnsteuer-kompakt.de' AS account_name,
    date,
    lk_sales_old_customers AS sales_old_customers,
    lk_sales_new_customers AS sales_new_customers
  FROM
    `planar-depth-242012.uploads.backend_sales`
  UNION ALL
  SELECT
    'steuergo.de' AS account_name,
    date,
    sg_sales_old_customers AS sales_old_customers,
    sg_sales_new_customers AS sales_new_customers
  FROM
    `planar-depth-242012.uploads.backend_sales`
  UNION ALL
  SELECT
    'steuererklaerung-student.de' AS account_name,
    date,
    sst_sales_old_customers AS sales_old_customers,
    sst_sales_new_customers AS sales_new_customers
  FROM
    `planar-depth-242012.uploads.backend_sales`
  UNION ALL
  SELECT
    'steuererklaerung-polizei.de' AS account_name,
    date,
    spo_sales_old_customers AS sales_old_customers,
    spo_sales_new_customers AS sales_new_customers
  FROM
    `planar-depth-242012.uploads.backend_sales`)
SELECT
  account_name,
  date,
  CAST (replace (sales_old_customers, ".", "") AS numeric) AS sales_old_customers,
  CAST (replace (sales_new_customers, ".", "") AS numeric) AS sales_new_customers
FROM
  sales_customers
ORDER BY
  date DESC
