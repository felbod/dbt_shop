
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
  , date_trunc (date(nps_answers.created_at), week(monday)) as date_week
  , extract(year from nps_answers.created_at) as date_year
  , length(nps_answers.nps_answer_comment) as nps_answer_comment_length

  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(fehler)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(server)") then 'fehler'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(hinweis.?\b)") then 'hinweis'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(korrektur|korrigieren)") then 'korrektur'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(falsch|fehl)eingabe") then 'fehleingabe'
      end as nps_comment__function__errors
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)englisc?h") then 'englisch'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)poln?isc?h") then 'polnisch'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)mutter.?sprach") then 'muttersprach'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)speak") then 'speak'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)translat") then 'translation'
      end as nps_comment__function__translations
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(steuer|spar)tipp?") then 'steuertipp'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)\b(tipp?|tipp?s)\b") then 'tipp'
      end as nps_comment__function__tips
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)rundet") then 'rundet'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)erstatt") then 'erstatt'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)berechn(ung|et)") then 'berechnung'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(nachzahl)") then 'nachzahl'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(rückzahl)") then 'rückzahl'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)geld.zur(ü|ue)ck") then 'geld_zurück'
      end as nps_comment__function__calculation
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(mail)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"@") then 'mail'
      end as nps_comment__function__email
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((men(ü|ue)|nutzer|programm|ziel|bediener|themen|durch|hin).?f(ü|ue)hrung)") then 'führung'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(f(ü|ue)hrung.+men(ü|u))") then 'führung'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\bf(ü|ue)hrung\b)") then 'führung'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\b(springt|springen|sprungen)\b)") then 'springt'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(men(ü|ue)struktur|untermen(ü|u))") then 'menü'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(zur(ü|ue)ck.?(geh|kehr|komm|bl(ä|ae)tt))") then 'zurück'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(navi)") then 'navi'
      end as nps_comment__function__navigation
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"\bEingabe.?\b") then 'eingabe'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(eingabe(maske|feld|option|system|vorrichtung))") then 'eingabemaske'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((zahlen|daten|datum.?|add?ress.*)eingabe)") then 'dateneingabe'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(eintrag|ein(.?zu.?|ge)trag)") then 'eintrag'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(drop.?down)") then 'dropdown'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((ä|ae)ndern)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(?i)(ländern)") then 'ändern'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(add?res)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(mail|@)") then 'adresse'
      end as nps_comment__function__data_entry
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(vorkasse)") then 'vorkasse'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(paypal)") then 'paypal'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(kreditkarte|visa)") then 'kreditkarte'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(lastschrift)") then 'lastschrift'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(berweisung)") then 'überweisung'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\brechnung\b)") then 'rechnung'
      end as nps_comment__function__payment
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(aus.*ge(worfen|schmissen))") then 'rausgeworfen'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(passwor)") then 'passwort'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(registrier)") then 'registrieren'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(ein.*log|log.?in|logging.in)") then 'login'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(aus.*log|log.?out|logging.out)") then 'logout'
      end as nps_comment__function__data_security
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(zertifikat|certificat)") then 'zertifikat'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\babgabe\b|abgeben)") then 'abgabe'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((ü|ue)bersend|versend|versand|verschick|abschick|abgeschick)") then 'versand'
      end as nps_comment__function__filing
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(daten.*abruf)") then 'datenabruf'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(beleg.*abruf)") then 'belegabruf'
      end as nps_comment__function__data_retrieval
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(daten.*(ü|ue)bern)") then 'datenübern'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(bertrag)") then 'übertrag'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(import)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(important)") then 'import'
      end as nps_comment__function__import
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((er.?|s)freundlich)") then 'nutzerfreundlich'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(verst(ä|ae)nd)") then 'verständlich'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(umst(ä|ae)nd)") then 'umständlich'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(kompliziert)") then 'kompliziert'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(versteh)") then 'versteh'
      end as nps_comment__category__usability
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(misstrau)") then 'misstrau'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\bsicher\b)") then 'sicher'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(nicht.sicher|unsicher|wirr)") then 'unsicher'
      end as nps_comment__category__reassurance
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(elster)") then 'elster'
      end as nps_comment__category__elster
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(kosten(los|frei))") then 'kostenlos'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(preiswert|billig)") then 'preiswert'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(kassier)") then 'kassieren'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(preise|preis(erh(ö|oe)hung|steigerung))") then 'preis'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(preis.*leistung)") then 'preis_leistung'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\b(teuer|teurer)\b)") then 'teuer'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(g(ü|ue)nstig)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(beg(ü|ue)nstigt|ung(ü|ue)nstig)") then 'günstig'
      end as nps_comment__category__pricing
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(bersicht)") then 'übersicht'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(orientieren|orientierung)") then 'orientieren'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(fenster)")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(hilf.fenster)") then 'fenster'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\bschrift\b)") then 'schrift'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(layout)") then 'layout'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(aufbau\b|aufgebaut)") then 'aufbau'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(scroll)") then 'scroll'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(bildschirm)") then 'bildschirm'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(design)") then 'design'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(farb)") then 'farbe'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(gra(f|ph)i)") then 'graphik'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(darstell)") then 'darstellung'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(optik|optisch)") then 'optisch'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(struktur)") then 'struktur'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(sichtbar)") then 'sichtbar'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(oberlfl(ä|ch)e)") then 'oberfläche'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\b(schwarz|rot|orange|gelb|gr(ü|ue)n)\b)") then 'farben'
      end as nps_comment__category__layout
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(betriebs.?system|apple|microsoft|windows|linux|android|\bi.?os\b|\bos.?x\b)") then 'betriebssystem'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(endgerät|mobil.*gerät|ipad|phone|handy|tablet|laptop|desktop|\bmobil|\bpc\b|\bmac\b)") then 'endgerät'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(browser|firefox|safari|edge|internet.?explorer|\bchrome\b)") then 'browser'
      end as nps_comment__category__device
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(st(u|ue|ü)rz)") then 'abstürz'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(h(ä|ae)ngt|aufgehangen|stockt)") then 'hängt'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(h(ä|ae)ngen.geblieben)") then 'hängt'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(langsam)") then 'langsam'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(neu.?start)") then 'neustart'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(lade.*(zeit|dauer|fehler))") then 'ladezeit'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(laden|lädt)") then 'laden'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(\bhar?kt|\bgehar?kt)") then 'hakt'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(server|server.?fehler)") then 'serverfehler'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(crash)") then 'crash'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((lange|ewig).warte)") then 'warten'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((lange.bearbeitungs|warte)zeit)") then 'wartezeit'
      end as nps_comment__category__performance
  , case
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)abk(ü|ue)rzung") then 'abkürzung'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(definiert|definition)") then 'definition'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((hinweis|info.*|hilfs)text)") then 'text'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(amts.?|fach.?|er.?|en.?|steuer.?)sprache\b") then 'sprache'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(amts.?|fach.?|er.?|en.?|steuer.?)deutsch\b") then 'sprache'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)chinesisch\b") then 'sprache'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)sprach")
        and not regexp_contains(nps_answers.nps_answer_comment, r"(entsprach|r(ü|ue)cksprache|muttersprach|englisc?h|polnisch)") then 'sprache'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(eingabe(tip|hilfe|kommentar))") then 'eingabe'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)beispiele") then 'beispiele'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)begriff")
        and not regexp_contains(nps_answers.nps_answer_comment, r"\bbegriffen\b") then 'begriff'  -- 'begriffen' fast immer i.S.v. 'verstanden'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)lesen") then 'lesen'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(beschr(ie|ei))") then 'beschreibung'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(hilfen|hilfe(stellung|text|feld|leiste|fenster))") then 'hilfe'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)((ausfüll|eingabe|informations|angaben)hilfe)") then 'hilfe'
      when regexp_contains(nps_answers.nps_answer_comment, r"(?i)(erkl(ä|ae)rt|erl(ä|e)uter|\berkl(ä|ae)rung)") then 'erklärung'
      end as nps_comment__category__wording
  , nps_answers.nps_answer_comment

from nps_answers
  left join users on nps_answers.user_id = users.user_id
  left join modules on nps_answers.nps_module_id = modules.nps_module_id
  left join controllers on nps_answers.controller_id = controllers.controller_id

order by nps_answers.nps_answer_id asc
