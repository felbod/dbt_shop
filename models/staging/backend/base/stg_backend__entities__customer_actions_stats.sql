
select
  kundenaction_id
  , kunde_id as user_id
  , controller as controller_id
  , steuerjahr as tax_year
  , datum_start
  , datum_abgabe
  , status
  , alter_em 
  , alter_ef
  , geschlecht_em
  , geschlecht_ef
  , wohnort_bundesland
  , wohnort_fa_gemeinde
  , partnerschaftsstatus
  , bank_blz
  , bank_name
  , berufsgruppe_em
  , berufsgruppe_ef
  , anlage_kind
  , anlage_v
  , anlage_n_em
  , anlage_n_ef
  , anlage_s_em
  , anlage_s_ef
  , anlage_g_em
  , anlage_g_ef
  , anlage_kap_em
  , anlage_kap_ef
  , anlage_r_em
  , anlage_r_ef
  , anlage_so_em
  , anlage_so_ef
  , veranlagungsart
  , einkommen_zve
  , spenden
  , handwerkerleistungen
  , haushaltsnahe_dienstleistungen
  , rueckerstattung
  , created_at
  , updated_at

from `planar-depth-242012.backend_data.backend__st_user_statistik`
