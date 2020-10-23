
with
  search_query_performance as (
    select
      *
      , safe_divide(cost_eur, clicks) as cost_per_click_eur
    from (
      select * except (cost_usd) from {{ref('stg_google_ads__search_query_performance')}})
    )
  , campaigns as (
--      select * from {{ref('stg_microsoft_ads__campaigns')}}
--      union all
      select * from {{ref('stg_google_ads__campaigns')}})
  , accounts as (
--      select * from {{ref('stg_microsoft_ads__accounts')}}
--      union all
      select * from {{ref('stg_google_ads__accounts')}})
  , search_query_conversions_customers as (
      select * from {{ref('stg_sea__search_query_conversions_customers')}}
    )

select
  accounts.platform_name
  , accounts.brand_name
  , search_query_performance.account_id
  , search_query_performance.campaign_id
  , accounts.account_name
  , campaigns.campaign_name
  , case
      when regexp_contains(campaigns.campaign_name, r"Display") then 'Display'
      when regexp_contains(campaigns.campaign_name, r"YouTube") then 'YouTube'
      when regexp_contains(campaigns.campaign_name, r"Shopping") then 'Shopping'
      else 'Search'
      end as campaign_type
  , case
      when regexp_contains(campaigns.campaign_name, r"Expats") then 'Expats'
      when regexp_contains(campaigns.campaign_name, r"Funnel") then 'Funnel'
      when regexp_contains(campaigns.campaign_name, r"Brand") then 'Brand'
      when regexp_contains(campaigns.campaign_name, r"Remarketing") then 'Remarketing'
      when regexp_contains(campaigns.campaign_name, r"AOTF") then 'AOTF'
      when regexp_contains(campaigns.campaign_name, r"RLSA") then 'RLSA'
      else 'Other'
      end as campaign_subtype
  , search_query_performance.date_day
  , extract (year from search_query_performance.date_day) as date_year
  , sum(search_query_performance.impressions) as impressions
  , sum(search_query_performance.clicks) as clicks
  , sum(search_query_performance.clicks)
    - sum(search_query_conversions_customers.clicks_old_customers)
        as clicks_new_customers
  , sum(search_query_conversions_customers.clicks_old_customers) as clicks_old_customers
  , sum(search_query_performance.conversions) as conversions
  , sum(search_query_conversions_customers.conversion_value_eur) as conversion_value_eur
  , sum(search_query_conversions_customers.conversion_value_eur_new_customers) as conversion_value_eur_new_customers
  , sum(search_query_conversions_customers.conversion_value_eur_old_customers) as conversion_value_eur_old_customers
  , sum(search_query_performance.cost_eur) as cost_eur
  , sum(search_query_performance.cost_eur)
    - (sum(search_query_performance.cost_per_click_eur) * sum(search_query_conversions_customers.clicks_old_customers))
    as cost_eur_new_customers
  , sum(search_query_performance.cost_per_click_eur) * sum(search_query_conversions_customers.clicks_old_customers)
    as cost_eur_old_customers

  , sum(search_query_conversions_customers.signups) as signups
  , sum(search_query_conversions_customers.starts) as starts
  , sum(search_query_conversions_customers.starts__new_customers) as starts__new_customers
  , sum(search_query_conversions_customers.starts__old_customers) as starts__old_customers
  , sum(search_query_conversions_customers.sales) as sales
  , sum(search_query_conversions_customers.sales__new_customers) as sales__new_customers
  , sum(search_query_conversions_customers.sales__old_customers) as sales__old_customers

from search_query_performance
  left join accounts on accounts.account_id = search_query_performance.account_id
  left join campaigns on campaigns.campaign_id = search_query_performance.campaign_id
  left join search_query_conversions_customers on search_query_performance.date_day = search_query_conversions_customers.date_day
    and search_query_performance.campaign_id = search_query_conversions_customers.campaign_id

group by
  accounts.platform_name
  , accounts.brand_name
  , search_query_performance.account_id
  , search_query_performance.campaign_id
  , search_query_performance.date_day
  , accounts.account_name
  , campaigns.campaign_name
  , date_year
