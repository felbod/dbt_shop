
with
  coupon_codes as (
    select * from {{ref('stg_backend__shop_gutscheincodes')}})
  , users as (
    select * from {{ref('stg_backend__entities__users')}})

select
  *
  , case
      when regexp_contains(coupon_codes.code, r"(?i)^f")
        and regexp_contains(coupon_codes.comment, r"(?i)^nps") then 'recommendation'
      when regexp_contains(coupon_codes.comment, r"(?i)^computer.*bild") then 'Computer Bild'
      end as campaign_type
  , case
      when date_diff(date(coupon_codes.date_day__redemption), date(users.created_at), year) = 0 then 'new'
      when date_diff(date(coupon_codes.date_day__redemption), date(users.created_at), year) > 0 then 'old'
    end as user_type

from coupon_codes
  left join users on coupon_codes.user_id = users.user_id

  order by
    date_day__redemption desc
    , controller_id
