
with campaign_data_google as (

  select *

    from {{ref('stg_google_ads__campaign_data')}}

),

campaign_data_microsoft as (

  select *

    from {{ref('stg_microsoft_ads__campaign_data')}}

)

select *

from campaign_data_google

union all

select *

from campaign_data_microsoft
