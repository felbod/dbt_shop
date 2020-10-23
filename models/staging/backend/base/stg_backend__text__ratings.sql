
select
  id as text_answer_id
  , text_id
  , jahr as tax_year
  , info_hilfreich as text_is_helpful
  , info_gut_uebersetzt as text_is_well_translated
  , kommentar as text_answer_comment
  , kundenaction_id as customer_action_id
  , user_id
  , controller as controller_id
  , culture as culture_short
  , created_at
  , kategorie as text_category
  , seiten_id as page_id
  , Vordruck_Name as tax_form_name

from `planar-depth-242012.backend_data.backend__st_user_text_bewertung`
