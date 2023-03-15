with cte_1 as
(select
day,
'sum_by_advertiser' as table,
count(*) as num_rows,
count(distinct advertiser_id) as num_advertiser_id,
'0' as num_campaign_id,
'0' as num_campaign_group_id,
'0' as num_creative_id,
'0' as num_group_id,
'0' as num_channel_id,
'0' as num_objective_id,
'0' as num_domain
, sum(coalesce(impressions,0))impressions
, sum(coalesce(clicks,0)) clicks
, sum(coalesce(click_conversions,0)) click_conversions
, sum(coalesce(click_order_value,0)) click_order_value
, sum(coalesce(data_cost,0)) data_cost
, sum(coalesce(media_cost,0)) media_cost
, sum(coalesce(fee_cost,0)) fee_cost
, sum(coalesce(partner_cost,0)) partner_cost
, sum(coalesce(data_spend,0)) data_spend
, sum(coalesce(media_spend,0)) media_spend
, sum(coalesce(platform_spend,0)) platform_spend
, sum(coalesce(video_spend,0)) video_spend
, sum(coalesce(legacy_spend,0)) legacy_spend
, sum(coalesce(vast_start,0)) vast_start
, sum(coalesce(vast_firstquartile,0)) vast_firstquartile
, sum(coalesce(vast_midpoint,0)) vast_midpoint
, sum(coalesce(vast_thirdquartile,0)) vast_thirdquartile
, sum(coalesce(vast_complete,0)) vast_complete
from summarydata.sum_by_advertiser_by_day	join advertisers using(advertiser_id)
where day>='2023-02-23' and day<'2023-03-01'
group by 1 order by 1
)

,cte_2 as (
select day,
sum(coalesce(views,0)) views1,
sum(coalesce(view_conversions,0)) view_conversions,
sum(coalesce(view_order_value,0)) view_order_value
, sum(coalesce(view_impressions,0)) view_impressions
, sum(coalesce(view_viewed,0)) view_viewed
, sum(coalesce(view_untrackable,0)) view_untrackable
, sum(coalesce(display_impressions,0)) display_impressions
, sum(coalesce(video_impressions, 0)) video_impressions
, sum(coalesce(new_to_file,0)) as new_to_file
, sum(coalesce(raw_visits,0)) as raw_visits
, sum(coalesce(raw_conversions,0)) as raw_conversions
, sum(coalesce(raw_order_value,0)) as  raw_order_value
from summarydata.sum_by_advertiser_by_day	join advertisers using(advertiser_id)
where day>='2023-02-23' and day<'2023-03-01'
group by 1 order by 1
)

--hll columns
,cte_3 as (
select day,
hll_cardinality(hll_union_agg(existing_site_visitors)) existing_site_visitors
,hll_cardinality(hll_union_agg(new_site_visitors)) new_site_visitors
,hll_cardinality(hll_union_agg(site_visitors)) site_visitors
,hll_cardinality(hll_union_agg(existing_users_reached)) existing_users_reached
,hll_cardinality(hll_union_agg(new_users_reached)) new_users_reached
,hll_cardinality(hll_union_agg(uniques)) uniques
,hll_cardinality(hll_union_agg(raw_existing_site_visitors)) as raw_existing_site_visitors
,hll_cardinality(hll_union_agg(raw_new_site_visitors))  as  raw_new_site_visitors
,hll_cardinality(hll_union_agg(visitors)) as  visitors
from summarydata.sum_by_advertiser_by_day	join advertisers using(advertiser_id)
where day>='2023-02-23' and day<'2023-03-01'
group by 1 order by 1 )



select * from cte_1 one 
left join cte_2 two using (day)
left join cte_3 three using (day)
order by 1