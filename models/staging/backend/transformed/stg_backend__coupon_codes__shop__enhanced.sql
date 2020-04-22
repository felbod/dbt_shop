
with
  coupon_codes as (
    select * from {{ref('stg_backend__shop_gutscheincodes')}})

/*
  backend_sales as (
    select * from {{ref('stg_backend__conversions__sales_revenue')}})
  , backend_signups as (
    select * from {{ref('stg_backend__conversions__signups')}})
  , backend_starts as (
    select * from {{ref('stg_backend__conversions__starts')}})
  , controllers as (
    select * from {{ref('stg_backend__entities__controllers')}})
*/

select
  *
  , case
      when regexp_contains(coupon_codes.code, r"(?i)^f")
        and regexp_contains(coupon_codes.comment, r"(?i)^nps") then 'recommendation'
      when regexp_contains(coupon_codes.comment, r"(?i)^computer.*bild") then 'Computer Bild'
      end as campaign_type

from coupon_codes

/*
from backend_sales
  left join backend_signups on backend_sales.date_day = backend_signups.date_day
    and backend_sales.controller_id = backend_signups.controller_id
  left join backend_starts on backend_sales.date_day = backend_starts.date_day
    and backend_sales.controller_id = backend_starts.controller_id
  left join controllers on backend_sales.controller_id = controllers.controller_id
*/

  order by
    date_day__redemption desc
    , controller_id
