
select
  id as nps_answer_id,
  st_nps_fragen_id as nps_module_id,
  user_id,
  kundenaction_id as customer_action_id,
  score as nps_answer_score,
  replace(replace(replace(replace(text, 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¼', 'ü'), 'Ã', 'Ü)') as nps_answer_comment,
  controller as controller_id,
  culture as culture_short,
  created_at,
  updated_at

from `planar-depth-242012.backend_data.backend__st_user_nps_bewertung`
