-- To do: Use currency exchange rates from backend to convert Google cost to EUR.

with platform_data as (

  select *

    from {{ref('mrts_consolidated__campaigns_customer_type')}}

),

backend_data as (

  select *

    from {{ref('stg_backend__accounts_customer_type')}}

)

SELECT
  platform_data.date,
  platform_data.account_name,
  sum(platform_data.impressions) as impressions,
  sum(platform_data.clicks) as clicks,
  sum(platform_data.conversions) as conversions,
  sum(platform_data.conversion_value_usd) as conversion_value_usd,
  sum(platform_data.cost_usd) as cost_usd,
  sum(platform_data.conversion_value_usd_new_customers) as conversion_value_usd_new_customers,
  sum(platform_data.conversion_value_usd_old_customers) as conversion_value_usd_old_customers,
  sum(platform_data.signups) as signups,
  sum(platform_data.starts_new_customers) as starts_new_customers,
  sum(platform_data.starts_old_customers) as starts_old_customers,
  sum(platform_data.sales_new_customers) as sales_new_customers,
  sum(platform_data.sales_old_customers) as sales_old_customers,

  sum(backend_data.revenue_old_customers) as backend_revenue_old_customers,
  sum(backend_data.revenue_new_customers) as backend_revenue_new_customers,
  sum(backend_data.sales_old_customers) as backend_sales_old_customers,
  sum(backend_data.sales_new_customers) as backend_sales_new_customers,
  sum(backend_data.starts_old_customers) as backend_starts_old_customers,
  sum(backend_data.starts_new_customers) as backend_starts_new_customers,
  sum(backend_data.signups) as backend_signups

FROM platform_data
  left join backend_data on platform_data.date = backend_data.date
    and platform_data.account_name = backend_data.account_name

group by
  platform_data.date,
  platform_data.account_name
