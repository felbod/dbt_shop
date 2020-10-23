
with
  platform_data as (
    select
  #    platform_name
  #   ,  account_id
      account_name
      , brand_name
      , date_day
      , sum(impressions) as impressions
      , sum(clicks) as clicks
      , sum(conversions) as conversions
      , sum(conversion_value_eur) as conversion_value_eur
      , sum(cost_eur) as cost_eur
      , sum(cost_eur_new_customers) as cost_eur_new_customers
      , sum(cost_eur_old_customers) as cost_eur_old_customers
      , sum(conversion_value_eur_new_customers) as conversion_value_eur_new_customers
      , sum(conversion_value_eur_old_customers) as conversion_value_eur_old_customers
      , sum(signups) as signups
      , sum(starts__new_customers) as starts__new_customers
      , sum(starts__old_customers) as starts__old_customers
      , sum(sales__new_customers) as sales__new_customers
      , sum(sales__old_customers) as sales__old_customers
      from {{ref('mrts_combined__campaigns__customer_type')}}
      group by
  #    platform_name
  #    , account_id
      account_name
      , brand_name
      , date_day)
  , backend_data as (
    select * from {{ref('stg_backend__conversions__customer_type')}})

select
  platform_data.date_day
  , extract (year from platform_data.date_day) as date_year
  , extract (isoyear from platform_data.date_day) as date_isoyear
  , extract (isoweek from platform_data.date_day) as date_isoweek
  , date_trunc (platform_data.date_day, week(monday)) as date_week
  , platform_data.account_name
  , platform_data.brand_name
  , sum(platform_data.impressions) as impressions
  , sum(platform_data.clicks) as clicks
  , sum(platform_data.conversions) as conversions
  , sum(platform_data.conversion_value_eur) as conversion_value_eur
  , - sum(platform_data.cost_eur) as cost_eur
  , - sum(platform_data.cost_eur_new_customers) as cost_eur_new_customers
  , - sum(platform_data.cost_eur_old_customers) as cost_eur_old_customers
  , sum(platform_data.conversion_value_eur_new_customers) as conversion_value_eur_new_customers
  , sum(platform_data.conversion_value_eur_old_customers) as conversion_value_eur_old_customers
  , sum(platform_data.signups) as signups
  , sum(platform_data.starts__new_customers) as starts__new_customers
  , sum(platform_data.starts__old_customers) as starts__old_customers
  , sum(platform_data.sales__new_customers) as sales__new_customers
  , sum(platform_data.sales__old_customers) as sales__old_customers
  , sum(backend_data.revenue_net_eur__old_customers) as backend_revenue_net_eur__old_customers
  , sum(backend_data.revenue_net_eur__new_customers) as backend_revenue_net_eur__new_customers
  , sum(backend_data.sales__old_customers) as backend_sales__old_customers
  , sum(backend_data.sales__new_customers) as backend_sales_new_customers
  , sum(backend_data.starts__old_customers) as backend_starts__old_customers
  , sum(backend_data.starts__new_customers) as backend_starts__new_customers
  , sum(backend_data.signups__new_customers) as backend_signups__new_customers
  , sum(backend_data.revenue_net_eur__new_customers) - sum(platform_data.cost_eur_new_customers) as real_profit_new_customers

from platform_data
  left join backend_data on platform_data.date_day = backend_data.date_day
    and platform_data.account_name = backend_data.brand_name

group by
  platform_data.date_day,
  account_name,
  brand_name,
  date_year
