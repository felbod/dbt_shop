SELECT
  CampaignId AS campaign_id,
  CampaignName AS campaign_name
FROM
  `planar-depth-242012.google_ads.Campaign_8644635112`
WHERE
  _DATA_DATE = _LATEST_DATE
ORDER BY
  CampaignId DESC
