
with campaign_data_google_ads as (

  select *

    from {{ref('stg_google_ads__campaigns_customer_type')}}

),

campaign_data_microsoft_ads as (

  select *

    from {{ref('stg_microsoft_ads__campaigns_customer_type')}}

)

select *

from campaign_data_google_ads

union all

select *

from campaign_data_microsoft_ads
