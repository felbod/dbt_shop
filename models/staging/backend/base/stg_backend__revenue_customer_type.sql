WITH
  revenue_customers AS (
  SELECT
    'lohnsteuer-kompakt.de' AS account_name,
    date,
    lk_gross_revenue_old_customers AS gross_revenue_old_customers,
    lk_gross_revenue_new_customers AS gross_revenue_new_customers
  FROM
    `planar-depth-242012.uploads.backend_revenue`
  UNION ALL
  SELECT
    'steuergo.de' AS account_name,
    date,
    sg_gross_revenue_old_customers AS gross_revenue_old_customers,
    sg_gross_revenue_new_customers AS gross_revenue_new_customers
  FROM
    `planar-depth-242012.uploads.backend_revenue`
  UNION ALL
  SELECT
    'steuererklaerung-student.de' AS account_name,
    date,
    sst_gross_revenue_old_customers AS gross_revenue_old_customers,
    sst_gross_revenue_new_customers AS gross_revenue_new_customers
  FROM
    `planar-depth-242012.uploads.backend_revenue`
  UNION ALL
  SELECT
    'steuererklaerung-polizei.de' AS account_name,
    date,
    spo_gross_revenue_old_customers AS gross_revenue_old_customers,
    spo_gross_revenue_new_customers AS gross_revenue_new_customers
  FROM
    `planar-depth-242012.uploads.backend_revenue`)
SELECT
  account_name,
  date,
  CAST (replace (replace (gross_revenue_old_customers, ".", ""), ",", ".") AS numeric) / 1.19 AS revenue_old_customers,
  CAST (replace (replace (gross_revenue_new_customers, ".", ""), ",", ".") AS numeric) / 1.19 AS revenue_new_customers
FROM
  revenue_customers
ORDER BY
  date DESC
