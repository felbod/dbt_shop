
select
  AdGroupId as adgroup_id,
  AdGroupName as adgroup_name

from
  `planar-depth-242012.google_ads__transfer.AdGroup_8644635112`

where
  _DATA_DATE = _LATEST_DATE

order by
  AdGroupId desc
