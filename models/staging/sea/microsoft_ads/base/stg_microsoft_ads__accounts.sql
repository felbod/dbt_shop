
  /*
  Siehe: STITCH > DOCS > DATA STRUCTURE > QUERYING APPEND-ONLY TABLES
  https://www.stitchdata.com/docs/data-structure/querying-append-only-tables#latest-version-every-row
  */

  select distinct
    'Microsoft Ads' as platform_name,
    o.id as account_id,
    o.name as account_name,
    currencycode as account_currency_code

    from `planar-depth-242012.microsoft_ads_stitch.accounts` as o

  inner join (

  select
    id,
    MAX(_sdc_sequence) AS seq,
    MAX(_sdc_batched_at) AS batch

  from `planar-depth-242012.microsoft_ads_stitch.accounts`

  group by id) as oo

  on o.id = oo.id
    and o._sdc_sequence = oo.seq
    and o._sdc_batched_at = oo.batch
