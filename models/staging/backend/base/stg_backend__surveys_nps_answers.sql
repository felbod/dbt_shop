
select
  id as nps_answer_id,
  st_nps_fragen_id as nps_questions_id,
  user_id,
  kundenaction_id as customer_action_id,
  score as nps_answer_score,
  text as nps_answer_text,
  controller as controller_id,
  culture as culture_short,
  created_at,
  updated_at

from `planar-depth-242012.backend_data.backend__st_user_nps_bewertung`
