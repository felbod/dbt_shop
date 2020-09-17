
/* https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models/

  {{
    config(
        materialized='incremental'
    )
  }}

  select
    *,
    my_slow_function(my_column)

  from raw_app_data.events

  {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_time > (select max(event_time) from {{ this }})

  {% endif %}

*/

{{
  config(
      materialized='incremental'
  )
}}

select
  *

from
  `planar-depth-242012.google_ads__transfer.Criteria_8644635112`

where
  HasQualityScore is true

  {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  and _DATA_DATE > (select max(_DATA_DATE) from {{ this }})

  {% endif %}

-- limit 1
