select
  CampaignId as campaign_id,
  CampaignName as campaign_name
from
  `planar-depth-242012.google_ads__transfer.Campaign_8644635112`
where
  _DATA_DATE = _LATEST_DATE
order by
  CampaignId desc
