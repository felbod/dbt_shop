
select
    brand_name
  , account_name
  , campaign_name
  , adgroup_name
  , display_name
  , replace(lower(strings), "+" , "") as string

/*
  , account_id
  , campaign_id
  , adgroup_id
--  , keyword_id
*/

--  , date_year
--  , date_isoyear
  , date_quarter
--  , date_month
--  , date_week
--  , date_isoweek
--  , date_day

  , sum(impressions) as impressions
  , sum(quality_score * impressions) as quality_score_impressions
--  , sum(quality_score) as quality_score

/*
  , sum(quality_score_calc) as quality_score_calc
  , sum(creative_quality_score_calc) as creative_quality_score_calc
  , sum(landingpage_quality_score_calc) as landingpage_quality_score_calc
  , sum(ctr_quality_score_calc) as ctr_quality_score_calc
*/

  , sum(quality_score_calc_impressions) as quality_score_calc_impressions
--  , sum(creative_quality_score_calc_impressions) as creative_quality_score_calc_impressions
--  , sum(landingpage_quality_score_calc_impressions) as landingpage_quality_score_calc_impressions
--  , sum(ctr_quality_score_calc_impressions) as ctr_quality_score_calc_impressions

  , sum(creative_quality_score_rel_impressions) as creative_quality_score_rel_impressions
  , sum(landingpage_quality_score_rel_impressions) as landingpage_quality_score_rel_impressions
  , sum(ctr_quality_score_rel_impressions) as ctr_quality_score_rel_impressions

  , sum(creative_quality_score_opt_impressions) as creative_quality_score_opt_impressions
  , sum(landingpage_quality_score_opt_impressions) as landingpage_quality_score_opt_impressions
  , sum(ctr_quality_score_opt_impressions) as ctr_quality_score_opt_impressions

from
  (
    select
      split(display_name, " ") as keywords
      , *
    from {{ref('stg_google_ads__quality_score__base')}}
    )
  , unnest(keywords) as strings

group by
    brand_name
  , account_name
  , campaign_name
  , adgroup_name
  , display_name
  , string

/*
  , account_id
  , campaign_id
  , adgroup_id      -- Aggregation by adgroup, because all keywords share the same creatives at this level
  , keyword_id
*/

--  , date_year
  , date_quarter
--  , date_isoyear
--  , date_month
--  , date_week
--  , date_isoweek
--  , date_day

order by
    brand_name
    , date_quarter desc
--  , date_month desc
--  , date_week desc
--  , date_day desc
