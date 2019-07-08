
with platform_data as (

  select *

    from {{ref('mrts_combined__campaigns_customer_type')}}

),

backend_data as (

  select *

    from {{ref('stg_backend__accounts_customer_type')}}

)

select
  platform_data.date_day,
  platform_data.account_name,
  sum(platform_data.impressions) as impressions,
  sum(platform_data.clicks) as clicks,
  sum(platform_data.conversions) as conversions,
  sum(platform_data.conversion_value_eur) as conversion_value_eur,
  sum(platform_data.cost_eur) as cost_eur,
  sum(platform_data.conversion_value_eur_new_customers) as conversion_value_eur_new_customers,
  sum(platform_data.conversion_value_eur_old_customers) as conversion_value_eur_old_customers,
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

from platform_data
  left join backend_data on platform_data.date_day = backend_data.date_day
    and platform_data.account_name = backend_data.account_name

group by
  platform_data.date_day,
  platform_data.account_name
