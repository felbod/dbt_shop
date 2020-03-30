
SELECT
  'shop'
  , sg.code
  , sg.verbraucht
  , sg.anzahl_einloesungen
  , sg.updated_at
  , sg.created_at
  , sg.bemerkung
  , s.kundenaction_id
--  , k.email
  , k.controller
  , u.created_at as registrierungsdatum
  , ka.datum as einloesedatum

FROM `steuern`.`shop_gutscheincode` sg

LEFT JOIN `kundendaten`.`shop` s on s.gutscheincode = sg.code
LEFT JOIN `kundendaten`.`kundenaction` ka on ka.id = s.kundenaction_id
-- LEFT JOIN `kundendaten`.`kunde` k on k.id = ka.kunde_id
LEFT JOIN `symfony_admin`.`sf_guard_user` u on k.email = u.username
  and k.controller = u.controller

WHERE s.gutscheincode is not null and sg.verbraucht <> 0

GROUP BY
  sg.code
  , s.kundenaction_id

order by updated_at desc
-- limit 20000


/* Abgabe-Codes

UNION (

  SELECT
    'abgabe'
    , ag.code
    , ag.verbraucht
    , count(*) as anzahl_einloesungen
    , sp.kundenaction_id
    , ag.updated_at
    , ag.created_at
    , ag.bemerkung
    , k.email
    , k.controller
    , u.created_at as registrierungsdatum
    , sp.einloesedatum

  FROM `steuern`.`admin_gutscheincode` ag

  LEFT JOIN `kundendaten`.`steuern_abgabecodes` sp on sp.abgabecode = ag.code
  LEFT JOIN `kundendaten`.`kundenaction` ka on ka.id = sp.kundenaction_id
  LEFT JOIN `kundendaten`.`kunde` k on k.id = ka.kunde_id
  LEFT JOIN `symfony_admin`.`sf_guard_user` u on k.email = u.username
    and k.controller = u.controller

  where (
    bemerkung like "%WK-Verlag%"
    or bemerkung like "%Groupon%")
    and verbraucht<>0
    and sp.kundenaction_id is not null

  group by ag.code

  order by updated_at desc

  --limit 20000

)

*/
