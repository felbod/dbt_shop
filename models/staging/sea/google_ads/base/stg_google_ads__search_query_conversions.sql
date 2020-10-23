
with
  search_query_conversions_usd as (
    select
      concat(cast (Date as string), cast (CampaignId as string), ConversionTypeName) as id_me
      , ExternalCustomerId as account_id
      , CampaignId as campaign_id
      , AdGroupId as adgroup_id
      , CriterionId as keyword_id
      , QueryMatchTypeWithVariant as match_type
      , Query as search_query
      , ConversionTypeName as conversion_name
      , Date as date_day
      , sum(AllConversions) as conversions
      , sum(AllConversionValue) as conversion_value_usd
      , if (FORMAT_DATE("%g", Date) <> regexp_extract(ConversionTypeName, r"[0-9][0-9]$"),
        0,
        1) as is_new_customers
      , if (ConversionTypeName = "Signup",
        sum(AllConversions),
        0) as signups
      , if (STARTS_WITH(ConversionTypeName, "Start R"),
        sum(AllConversions),
        0) as starts
      , if (STARTS_WITH(ConversionTypeName, "Sale R"),
        sum(AllConversions),
        0) as sales
    from
      `planar-depth-242012.google_ads__transfer.SearchQueryConversionStats_8644635112`
    group by
      date_day
      , account_id
      , campaign_id
      , adgroup_id
      , keyword_id
      , match_type
      , search_query
      , conversion_name
    )

  , exchange_rates as (
    select * from {{ref('stg_external__exchange_rates_all_dates')}})

    /*
      - For quality assurance
      - To find dates without values
      - These will appear as NONE in the result
      - Excluding today
      */
  , dates as (
    select date_day
      from unnest(
          generate_date_array (date('2018-01-01'), date_sub(current_date, interval 1 day), interval 1 day)
      ) as date_day
    )

select
  search_query_conversions_usd.account_id
  , search_query_conversions_usd.campaign_id
  , search_query_conversions_usd.adgroup_id
  , search_query_conversions_usd.keyword_id
  , search_query_conversions_usd.match_type
  , search_query_conversions_usd.search_query
  , dates.date_day
  , search_query_conversions_usd.conversion_name
  , search_query_conversions_usd.conversions
  , search_query_conversions_usd.conversion_value_usd / exchange_rate_eur_usd as conversion_value_eur
  , search_query_conversions_usd.is_new_customers
  , search_query_conversions_usd.signups
  , search_query_conversions_usd.starts
  , search_query_conversions_usd.sales

from
  search_query_conversions_usd
  left join exchange_rates on exchange_rates.date_day = search_query_conversions_usd.date_day
  full join dates on dates.date_day = search_query_conversions_usd.date_day
