
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
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(fehler|hinweis.?\b)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(server)")
        then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(korrektur|korrigieren)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(falsch|fehl)eingabe") then '1'
    end as nps_comment__function__errors
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)\b(tipp?|tipp?s)\b") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(steuer|spar)tipp?") then '1'
    end as nps_comment__function__tips
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)rundet") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)erstatt") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)berechn(ung|et)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(nach|rück)zahl") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)geld.zur(ü|ue)ck") then '1'
      end as nps_comment__function__calculation
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(mail)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"@")
        then '1'
      end as nps_comment__function__email
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((men(ü|ue)|nutzer|programm|ziel|bediener|themen|durch|hin).?f(ü|ue)hrung)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(f(ü|ue)hrung.+men(ü|u))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\bf(ü|ue)hrung\b)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(men(ü|ue)struktur|untermen(ü|u))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(zurück.?(geh|spring|kehr|komm|bl(ä|ae)tt))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(navi)") then '1'
      end as nps_comment__function__navigation
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"\bEingabe.?\b") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(eingabe(maske|feld|option|system|vorrichtung))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((zahlen|daten|datum.?|add?ress.*)eingabe)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(eintrag|ein(.?zu.?|ge)trag)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(drop.?down)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(ändern)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(?i)(ländern)")
        then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(add?res)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(mail|@)")
        then '1'
      end as nps_comment__function__data_entry
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(paypal|kreditkarte|visa|lastschrift|berweisung|\brechnung\b)") then '1'
      end as nps_comment__function__payment
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((daten|beleg).*abruf)") then '1'
      end as nps_comment__function__data_retrieval
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(daten.*(ü|ue)bern)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(bertrag)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(import)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(important)")
        then '1'
      end as nps_comment__function__import
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((er.?|s)freundlich)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((ver|um)st(ä|ae)nd)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(kompliziert|versteh)") then '1'
      end as nps_comment__category__usability
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(misstrau)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\bsicher\b)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(nicht.sicher|unsicher|wirr)") then '1'
      end as nps_comment__category__reassurance
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(elster)") then '1'
      end as nps_comment__category__elster
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(preiswert|billig|kosten(los|frei))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(kassier)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(preise|preis(erh(ö|oe)hung|steigerung))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(preis.*leistung)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\b(teuer|teurer)\b)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(g(ü|ue)nstig)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(beg(ü|ue)nstigt|ung(ü|ue)nstig)")
        then '1'
      end as nps_comment__category__pricing
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(bersicht)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(scroll)") then '1'
      end as nps_comment__category__layout
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(endgerät|phone|handy|tablet|laptop|\bpc\b|\bmac\b)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(mobil.*gerät)") then '1'
      end as nps_comment__category__device
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(st(u|ue|ü)rz|h(ä|ae)ngt|aufgehangen|langsam)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(neu.?start)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(h(ä|ae)ngen.geblieben)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(lade.*(zeit|dauer|fehler)|laden|lädt|stockt)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\bhar?kt|\bgehar?kt)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(server|server.?fehler)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(crash)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((lange|ewig).warte)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((lange.bearbeitungs|warte)zeit)") then '1'
      end as nps_comment__category__performance
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(hilfen|hilfe(stellung|text|feld|leiste))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((hinweis|info.*|hilfs)text)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((ausfüll|eingabe|informations|angaben)hilfe)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(eingabe(tip|hilfe|kommentar))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(erkl(ä|ae)rt|erl(ä|e)uter|\berkl(ä|ae)rung)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(beschr(ie|ei))") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)beispiele") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)chinesisch\b") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)abk(ü|ue)rzung") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(definiert|definition)") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)lesen") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)begriff")
        and not regexp_contains(nps_answers.nps_answer_comment, r"\bbegriffen\b")
        then '1'  -- 'begriffen' fast immer i.S.v. 'verstanden'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(amts.?|fach.?|er.?|en.?|steuer.?)sprache\b") then '1'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(amts.?|fach.?|er.?|en.?|steuer.?)deutsch\b") then '1'
      end as nps_comment__category__wording
  , nps_answers.nps_answer_comment

from nps_answers
  left join users on nps_answers.user_id = users.user_id
  left join modules on nps_answers.nps_module_id = modules.nps_module_id
  left join controllers on nps_answers.controller_id = controllers.controller_id

order by nps_answers.nps_answer_id asc
