
with
  nps_answers as (
    select * from {{ref('stg_backend__surveys__nps_answers')}})
  , users as (
    select * from {{ref('stg_backend__entities__users')}})
  , modules as (
    select * from {{ref('stg_backend__surveys__nps_modules')}})
  , controllers as (
    select * from {{ref('stg_backend__entities__controllers')}})

select
  nps_answers.nps_answer_id
  , nps_answers.nps_module_id
  , modules.module_text
  , concat(nps_answers.nps_module_id, '_', modules.module_text) as question_id_stage
  , nps_answers.user_id
  , date_diff(date(nps_answers.created_at), date(users.created_at), year) as user_seniority
  , case
      when date_diff(date(nps_answers.created_at), date(users.created_at), year) = 0 then 'new'
      when date_diff(date(nps_answers.created_at), date(users.created_at), year) > 0 then 'old'
    end as user_type
  , users.created_at as user_created_at
  , nps_answers.nps_answer_score
  , case
      when nps_answers.nps_answer_score > 8 then 100
      when nps_answers.nps_answer_score > 6 then 0
      when nps_answers.nps_answer_score > 0 then -100
    end as nps_weight
  , nps_answers.controller_id
  , controllers.brand_name
  , nps_answers.culture_short
  , nps_answers.created_at
  , date(nps_answers.created_at) as date_day
  , extract(year from nps_answers.created_at) as date_year
  , length(nps_answers.nps_answer_comment) as nps_answer_comment_length
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(fehler|hinweis.?\b)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(korrektur|korrigieren)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(falsch|fehl)eingaben") then '1'
    end as nps_comment_function_errors
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)\b(tipp?|tipp?s)\b") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(steuer|spar)tipp?") then '1'
    end as nps_comment_function_tips
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)erstatt") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)berechn(ung|et)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(nach|rück)zahl") then '1'
      end as nps_comment_function_calculation
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(mail)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"@")
        then '1'
      end as nps_comment_function_email
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((menü|menue|nutzer|programm|ziel|bediener|themen|durch|hin).?f(ü|ue)hrung)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(f(ü|ue)hrung.+men(ü|u))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\bf(ü|ue)hrung\b)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(navi)") then '1'
      end as nps_comment_function_navigation
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(daten.*übern)") then '1'
      end as nps_comment_function_import
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(elster)") then '1'
      end as nps_comment_category_elster
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(preiswert|billig|kosten(los|frei))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\bteuer\b)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(günstig)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(begünstigt|ungünstig)")
        then '1'
      end as nps_comment_category_pricing
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(st(u|ue|ü)rz|hängt|aufgehangen|langsam)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(lade.*(zeit|dauer|fehler)|laden)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\bhar?kt|\bgehar?kt)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(wartezeit)") then '1'
      end as nps_comment_category_performance
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(hilfen|hilfe(stellung|text|feld|leiste)|(ausfüll|eingabe|informations|angaben)hilfe)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(erkl(ä|ae)rt|erl(ä|e)uter|\berkl(ä|ae)rung)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(beschr(ie|ei))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)beispiele") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)chinesisch\b") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)abk(ü|ue)rzung") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)lesen") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)begriff")
        and not regexp_contains(nps_answers.nps_answer_comment, r"\bbegriffen\b")
        then '1'  -- 'begriffen' fast immer i.S.v. 'verstanden'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(amts.?|fach.?|er.?|en.?|steuer.?)sprache\b") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(amts.?|fach.?|er.?|en.?|steuer.?)deutsch\b") then '1'
      end as nps_comment_category_wording
  , nps_answers.nps_answer_comment

from nps_answers
  left join users on nps_answers.user_id = users.user_id
  left join modules on nps_answers.nps_module_id = modules.nps_module_id
  left join controllers on nps_answers.controller_id = controllers.controller_id

order by nps_answers.nps_answer_id asc
