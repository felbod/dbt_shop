
select
  user_id,
  antwort_id as source_answers_id,
  kundenaction_id as customer_action_id,
  text as source_answer_text,
  controller as controller_id,
  culture as culture_short,
  created_at,
  updated_at

from `planar-depth-242012.backend_data.backend__st_user_source_umfrage`
