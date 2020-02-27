
select
  id as controller_id,
  controller_text as controller_short,
  case
    when id = 4 then 'lohnsteuer-kompakt.de'
    when id = 5 then 'steuergo.de'
    when id = 501 then 'steuererklaerung-student.de'
    when id = 504 then 'steuererklaerung-polizei.de'
  end as brand_name

from `planar-depth-242012.backend_constants.backend_code_controller_text`
