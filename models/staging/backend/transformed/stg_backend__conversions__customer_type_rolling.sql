/*  https://medium.com/@rodolfo.marcos07/how-to-easy-understand-analytics-functions-on-bigquery-f9c81f7e9fbb
    https://www.sisense.com/blog/outlier-detection-in-sql/ */

select
  date_day,
  extract(dayofweek from date_day) as date_day_dayofweek,
  controller_id,
  signups__new_customers

  , avg(signups__new_customers)
    OVER (
      partition by controller_id
      ORDER BY date_day asc
      ROWS BETWEEN 3 PRECEDING AND 3 following)
    as signups__new_customers__7_ma

  , avg(signups__new_customers)
    OVER (
      partition by controller_id
      ORDER BY date_day asc
      ROWS BETWEEN 182 PRECEDING AND 182 following)
    as signups__new_customers__365_ma

/*  , case
    when
    abs((signups__new_customers -
      AVG(signups__new_customers)
        OVER (
          partition by controller_id
          ORDER BY date_day asc
          ROWS BETWEEN 27 PRECEDING AND CURRENT ROW))
      / stddev(signups__new_customers)
        OVER (
          partition by controller_id
          ORDER BY date_day asc
          ROWS BETWEEN 27 PRECEDING AND CURRENT ROW))
     >= 2.576 then signups__new_customers -- Z-score, two-tailed: 1.645 (95%), 2.576 (99%)
  end AS signups__new_customers_avg_28_outliers_99
*/
from {{ref('stg_backend__conversions__signups')}}

order by controller_id, date_day desc
