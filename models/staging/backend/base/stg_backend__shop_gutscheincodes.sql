
select
code_art as code_type
, code
, verbraucht
, anzahl_einloesungen
, kundenaction_id
, updated_at as date_day__redemption
, created_at
, bemerkung as comment
, controller as controller_id
, registrierungsdatum as signup_date
, einloesedatum as redemption_date
, user_id
, kaufpreis_netto as price_paid
, artikelpreis_netto as basket_value

from `planar-depth-242012.backend_data.backend__shop_gutscheincodes`
