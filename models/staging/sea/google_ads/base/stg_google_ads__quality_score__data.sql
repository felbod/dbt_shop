/*
  example for incremental models
  https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models/
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

  -- only save criteria with quality score, i.e. keywords
  HasQualityScore is true

  {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  and _DATA_DATE > (select max(_DATA_DATE) from {{ this }})

  {% endif %}
