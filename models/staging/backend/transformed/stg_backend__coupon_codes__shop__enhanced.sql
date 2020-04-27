
select
  *
  , case
      when regexp_contains(code, r"(?i)^f")
        and regexp_contains(comment, r"(?i)^nps") then 'Recommendation'
      when regexp_contains(comment, r"(?i)computer.*bild") then 'Computer Bild'
      when regexp_contains(comment, r"(?i)daily.*deal") then 'Daily Deal'
      when regexp_contains(comment, r"(?i)groupon") then 'Groupon'
      when regexp_contains(comment, r"(?i)facebook") then 'Facebook'
      when regexp_contains(comment, r"(?i)zenjob") then 'Zenjob'
      when regexp_contains(comment, r"(?i)darweesh|toumpan|cobalt") then 'Großkunden'
      when regexp_contains(comment, r"(?i)test") then 'Testing'
      else 'Other'
      end as campaign_type
  , case
      when date_diff(date(redemption_date), date(signup_date), year) = 0 then 'new'
      when date_diff(date(redemption_date), date(signup_date), year) > 0 then 'old'
    end as user_type

from {{ref('stg_backend__shop_gutscheincodes')}}

  order by
    redemption_date desc
    , controller_id
