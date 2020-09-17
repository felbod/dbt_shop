
/*
Qualitätsfaktor - Umrechnung von Worten zu Zahlen

Qualitätsfaktor = 1 + Wertung Klickrate + Wertung Relevanz + Wertung Zielseitenerfahrung

Es gilt für Klickrate und Zielseitenerfahrung:
- überdurchschnittlich: 3,5
- durchschnittich: 1,75
- unterdurchschnittlich: 0
und für Relevanz:
- überdurchschnittlich: 2
- durchschnittlich: 1
- unterdurchschnittlich: 0

Am Beispiel von Abbildung 6 ergibt sich also: 1 + 3,5 + 2 + 1,75 = 8,25 und damit abgerundet ein Qualitätsfaktor von 8.
Ebenfalls nicht eindeutig interpretiert sind die Werte der Skala von 1 bis 10, die den Qualitätsfaktor angeben. Als Richtlinien können Sie eine Einteilung wie folgt vornehmen: Ein Qualitätsfaktor von 6 kann derzeit als Durchschnitt angesehen werden.
Entsprechend sind Qualitätsfaktoren von 7 aufwärts überdurchschnittlich, Qualitätsfaktoren von S abwärts unterdurchschnittlich. Qualitätsfaktoren von 3 bis 4 deuten zumeist auf grundlegende Probleme wie eine ungenügende Kontostruktur inklusive schlechter Landingpages hin. Bei Qualitätsfaktoren von 1 und 2 kommt es in der Regel nur noch zu sporadischen Anzeigenschaltungen.

Siehe Website Boosting 11-12.2017, S. 53"
*/

with
  quality_score as (
    select
        *
      , round(creative_quality_score_calc + landingpage_quality_score_calc + ctr_quality_score_calc) as quality_score_calc
--      , (creative_quality_score_rel + landingpage_quality_score_rel + ctr_quality_score_rel) as quality_score_rel
    from (
      select
          ExternalCustomerId as account_id
        , CampaignId as campaign_id
        , AdGroupId as adgroup_id
        , CriterionId as keyword_id
        , CriteriaType as criterion_type
        , DisplayName as display_name
        , CreativeQualityScore as creative_quality_score_text
        , PostClickQualityScore as landingpage_quality_score_text
        , SearchPredictedCtr as ctr_quality_score_text

        , (case
            when CreativeQualityScore = 'ABOVE_AVERAGE' then 2
            when CreativeQualityScore = 'AVERAGE' then 1
            when CreativeQualityScore = 'BELOW_AVERAGE' then 0
          end
          + 1/3) as creative_quality_score_calc
        , (case
            when PostClickQualityScore = 'ABOVE_AVERAGE' then 3.5
            when PostClickQualityScore = 'AVERAGE' then 1.75
            when PostClickQualityScore = 'BELOW_AVERAGE' then 0
          end
          + 1/3) as landingpage_quality_score_calc
        , (case
            when SearchPredictedCtr = 'ABOVE_AVERAGE' then 3.5
            when SearchPredictedCtr = 'AVERAGE' then 1.75
            when SearchPredictedCtr = 'BELOW_AVERAGE' then 0
          end
          + 1/3) as ctr_quality_score_calc

        , (case
            when CreativeQualityScore = 'ABOVE_AVERAGE' then 2
            when CreativeQualityScore = 'AVERAGE' then 1
            when CreativeQualityScore = 'BELOW_AVERAGE' then 0
          end / 2) as creative_quality_score_rel
        , (case
            when PostClickQualityScore = 'ABOVE_AVERAGE' then 3.5
            when PostClickQualityScore = 'AVERAGE' then 1.75
            when PostClickQualityScore = 'BELOW_AVERAGE' then 0
          end / 3.5) as landingpage_quality_score_rel
        , (case
            when SearchPredictedCtr = 'ABOVE_AVERAGE' then 3.5
            when SearchPredictedCtr = 'AVERAGE' then 1.75
            when SearchPredictedCtr = 'BELOW_AVERAGE' then 0
          end / 3.5) as ctr_quality_score_rel

        , QualityScore as quality_score
        , _DATA_DATE as date_day
        from {{ref('stg_google_ads__quality_score_data')}}
        where
          HasQualityScore is true
        order by _DATA_DATE desc
        )
      )

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

  , accounts as (
      select * from {{ref('stg_google_ads__accounts')}})

  , campaigns as (
      select * from {{ref('stg_google_ads__campaigns')}})

  , adgroups as (
      select * from {{ref('stg_google_ads__adgroups')}})

  , keywords as (
      select * from {{ref('stg_google_ads__keyword_performance')}})

select
    brand_name
  , account_name
  , campaign_name
  , adgroup_name
  , account_id
  , campaign_id
  , adgroup_id
--  , keyword_id
--  , display_name
--  , date_day
  , date_year
  , date_isoyear
  , date_isoweek
  , date_week
  , sum(impressions) as impressions
  , sum(quality_score * impressions) as quality_score_impressions
  , sum(quality_score) as quality_score

  , sum(quality_score_calc) as quality_score_calc
  , sum(creative_quality_score_calc) as creative_quality_score_calc
  , sum(landingpage_quality_score_calc) as landingpage_quality_score_calc
  , sum(ctr_quality_score_calc) as ctr_quality_score_calc

  , sum(quality_score_calc * impressions) as quality_score_calc_impressions
  , sum(creative_quality_score_calc * impressions) as creative_quality_score_calc_impressions
  , sum(landingpage_quality_score_calc * impressions) as landingpage_quality_score_calc_impressions
  , sum(ctr_quality_score_calc * impressions) as ctr_quality_score_calc_impressions
/*
  , sum(creative_quality_score_rel) as creative_quality_score_rel
  , sum(landingpage_quality_score_rel) as landingpage_quality_score_rel
  , sum(ctr_quality_score_rel) as ctr_quality_score_rel
  , sum(quality_score_rel) as quality_score_rel
*/
  , sum(creative_quality_score_rel * impressions) as creative_quality_score_rel_impressions
  , sum(landingpage_quality_score_rel * impressions) as landingpage_quality_score_rel_impressions
  , sum(ctr_quality_score_rel * impressions) as ctr_quality_score_rel_impressions
--  , sum(quality_score_rel * impressions) as quality_score_rel_impressions

  , sum((1 - creative_quality_score_rel) * impressions) as creative_quality_score_opt_impressions
  , sum((1 - landingpage_quality_score_rel) * impressions) as landingpage_quality_score_opt_impressions
  , sum((1 - ctr_quality_score_rel) * impressions) as ctr_quality_score_opt_impressions
--  , sum((1 - quality_score_rel) * impressions) as quality_score_opt_impressions

  from (
    select
        accounts.brand_name
      , accounts.account_name
      , campaigns.campaign_name
      , adgroups.adgroup_name
      , quality_score.account_id
      , quality_score.campaign_id
      , quality_score.adgroup_id
--      , quality_score.keyword_id
      , quality_score.display_name

      , dates.date_day
      , extract (year from dates.date_day) as date_year
      , extract (isoyear from dates.date_day) as date_isoyear
      , extract (isoweek from dates.date_day) as date_isoweek
      , date_trunc (dates.date_day, week(monday)) as date_week

      , quality_score.creative_quality_score_calc
      , quality_score.landingpage_quality_score_calc
      , quality_score.ctr_quality_score_calc
      , quality_score.quality_score_calc

      , quality_score.creative_quality_score_rel
      , quality_score.landingpage_quality_score_rel
      , quality_score.ctr_quality_score_rel
--      , quality_score.quality_score_rel

      , quality_score.quality_score
      , keywords.impressions

    from
      quality_score
      left join accounts on quality_score.account_id = accounts.account_id
      left join campaigns on quality_score.campaign_id = campaigns.campaign_id
      left join adgroups on quality_score.adgroup_id = adgroups.adgroup_id
      left join keywords on quality_score.adgroup_id = keywords.adgroup_id
        and quality_score.keyword_id = keywords.keyword_id
        and quality_score.date_day = keywords.date_day
      full join dates on quality_score.date_day = dates.date_day
    where
      keywords.impressions is not null
  )

group by
    brand_name
  , account_name
  , campaign_name
  , adgroup_name
  , account_id
  , campaign_id
  , adgroup_id      -- Aggregation by adgroup, because all keywords share the same creatives at this level
--  , keyword_id
--  , display_name
--  , date_day
  , date_year
  , date_isoyear
  , date_isoweek
  , date_week

order by
    brand_name
  , date_week desc
--  , date_day desc
