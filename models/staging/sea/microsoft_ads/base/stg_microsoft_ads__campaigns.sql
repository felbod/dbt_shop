
/*
Siehe: STITCH > DOCS > DATA STRUCTURE > QUERYING APPEND-ONLY TABLES
https://www.stitchdata.com/docs/data-structure/querying-append-only-tables#latest-version-every-row
*/

select distinct
  o.id as campaign_id,
  o.name as campaign_name

  

  from `planar-depth-242012.microsoft_ads_stitch.campaigns` as o

inner join (

select
  id,
  MAX(_sdc_sequence) AS seq,
  MAX(_sdc_batched_at) AS batch

from `planar-depth-242012.microsoft_ads_stitch.campaigns`

group by id) as oo

on o.id = oo.id
  and o._sdc_sequence = oo.seq
  and o._sdc_batched_at = oo.batch
