
with campaign_performance as (

  select *

    from {{ref('stg_microsoft_ads__campaign_performance')}}

  union all

  select *

    from {{ref('stg_google_ads__campaign_performance')}}

),

campaign_conversions_customers as (

  select *

    from {{ref('stg_microsoft_ads__campaign_conversions_customers')}}

  union all

  select *

    from {{ref('stg_google_ads__campaign_conversions_customers')}}

),

campaigns as (

  select *

    from {{ref('stg_microsoft_ads__campaigns')}}

  union all

  select *

    from {{ref('stg_google_ads__campaigns')}}

),

accounts as (

  select *

    from {{ref('stg_microsoft_ads__accounts')}}

  union all

  select *

    from {{ref('stg_google_ads__accounts')}}

)

select
  accounts.platform_name,
  campaign_performance.account_id,
  campaign_performance.campaign_id,

  accounts.account_name,

  campaigns.campaign_name,

  campaign_performance.date_day,

  sum(campaign_performance.impressions) as impressions,
  sum(campaign_performance.clicks) as clicks,
  sum(campaign_performance.conversions) as conversions,
  sum(campaign_performance.conversion_value_eur) as conversion_value_eur,

  sum(campaign_performance.cost_eur) as cost_eur,
  sum(campaign_performance.cost_per_click_eur) * sum(campaign_conversions_customers.clicks_old_customers) as cost_eur_old_customers,
  sum(cost_eur) - sum(campaign_performance.cost_per_click_eur) * sum(campaign_conversions_customers.clicks_old_customers) as cost_eur_new_customers,

  sum(campaign_conversions_customers.conversion_value_eur_new_customers) as conversion_value_eur_new_customers,
  sum(campaign_conversions_customers.conversion_value_eur_old_customers) as conversion_value_eur_old_customers,
  sum(campaign_conversions_customers.signups) as signups,
  sum(clicks) - sum(clicks_old_customers) as clicks_new_customers,
  sum(campaign_conversions_customers.clicks_old_customers) as clicks_old_customers,
  sum(campaign_conversions_customers.starts_new_customers) as starts_new_customers,
  sum(campaign_conversions_customers.starts_old_customers) as starts_old_customers,
  sum(campaign_conversions_customers.sales_new_customers) as sales_new_customers,
  sum(campaign_conversions_customers.sales_old_customers) as sales_old_customers

from campaign_performance
  left join accounts on accounts.account_id = campaign_performance.account_id
  left join campaigns on campaigns.campaign_id = campaign_performance.campaign_id
  left join campaign_conversions_customers on campaign_performance.date_day = campaign_conversions_customers.date_day
    and campaign_performance.campaign_id = campaign_conversions_customers.campaign_id

group by
  accounts.platform_name,
  campaign_performance.account_id,
  campaign_performance.campaign_id,
  campaign_performance.date_day,
  accounts.account_name,
  campaigns.campaign_name
