
with
  campaign_conversions_usd as (
    select
      concat(cast (Date as string), cast (CampaignId as string), ConversionTypeName) as id_me
      , ExternalCustomerId as account_id
      , CampaignId as campaign_id
      , Date as date_day
      , ConversionTypeName as conversion_name
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
      `planar-depth-242012.google_ads__transfer.p_CampaignCrossDeviceConversionStats_8644635112`
    group by
      account_id,
      campaign_id,
      date_day,
      conversion_name)

  , exchange_rates as (
    select * from {{ref('stg_external__exchange_rates__all_dates')}})

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
      ) as date_day)

select
  campaign_conversions_usd.account_id
  , campaign_conversions_usd.campaign_id
  , dates.date_day
  , campaign_conversions_usd.conversion_name
  , campaign_conversions_usd.conversions
  , campaign_conversions_usd.conversion_value_usd / exchange_rate_eur_usd as conversion_value_eur
  , campaign_conversions_usd.is_new_customers
  , campaign_conversions_usd.signups
  , campaign_conversions_usd.starts
  , campaign_conversions_usd.sales

from
  campaign_conversions_usd
  left join exchange_rates on exchange_rates.date_day = campaign_conversions_usd.date_day
  full join dates on dates.date_day = campaign_conversions_usd.date_day
