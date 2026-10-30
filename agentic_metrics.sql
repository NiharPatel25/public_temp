-- dependencies: `etsy-data-warehouse-prod.transaction_mart.all_receipts`, `etsy-data-warehouse-prod.transaction_mart.all_transactions`, etsy-data-warehouse-prod.rollups.transaction_reviews, etsy-data-warehouse-prod.etsy_shard.offsite_checkout_cart
-- description: Creating offsite transaction tables for agentic commerce
-- data_access_bucket: etsy-data-warehouse-prod.rollups.agentic_offsite_listing_gms=4 | etsy-data-warehouse-prod.rollups.all_purchases_mapped_user=4 | etsy-data-warehouse-prod.rollups.user_agentic_purch=4 | etsy-data-warehouse-prod.rollups.offsite_purch_freq=4 | etsy-data-warehouse-prod.rollups.agentic_referral_purch_freq=4
-- run_type: full_refresh
-- safe_to_skip_during_incident: true

/*
Purpose
-------
Build the reporting layer used to measure purchases originating from agentic
referrals and offsite checkout experiences.

The pipeline:
1. Assigns each buyer their latest monthly buyer segment.
2. Enriches purchases with listing, taxonomy, GMS, and review attributes.
3. Produces comparable channel-level reporting for offsite checkout, AI
   referrals, PLA traffic, and Etsy overall.
4. Determines whether an offsite buyer previously purchased on Etsy, using
   mapped-user identity where available and address matching as a fallback.
5. Measures subsequent Etsy orders and GMS over 14-, 30-, 90-, 180-, and
   360-day observation windows.

Important: Some rows in the combined reporting table are overlapping comparison
groups. They should not be summed together as mutually exclusive channels.
*/

BEGIN

-- Select the most recent buyer segment recorded for each mapped user and month.
-- Keeping one month-end record prevents multiple segment assignments downstream.
create temporary table mo_end_buyer_segments 
cluster by mapped_user_id as (
  with mo_end_buyer_segments as (
    select 
      format_timestamp('%Y-%m', as_of_date) as year_month
      ,as_of_date
      ,mapped_user_id
      ,buyer_segment
    from `etsy-data-warehouse-prod.rollups.buyer_segment_monthly`
    where as_of_date >= parse_date('%Y-%m-%d', '2025-01-01')
  )

  select *
  from mo_end_buyer_segments
  qualify row_number() over (partition by year_month, mapped_user_id order by as_of_date desc) = 1
);

-- Translate mapped-user buyer segments to individual user IDs so that purchases
-- can be enriched regardless of which identity field is populated at checkout.
create temporary table mup_mapping
cluster by user_id as (
  select
    y.year_month
    ,u.user_id
    ,u.mapped_user_id
    ,mu.is_guest
    ,y.buyer_segment
  from `etsy-data-warehouse-prod.user_mart.user_mapping` u
    inner join `etsy-data-warehouse-prod.user_mart.mapped_user_profile` mu 
      on u.mapped_user_id = mu.mapped_user_id
    inner join mo_end_buyer_segments y
      on u.mapped_user_id = y.mapped_user_id
  qualify row_number() over (
    partition by user_id, year_month 
    order by y.as_of_date desc
  ) = 1
);


-- Build the listing-level fact table for purchases completed through agentic
-- offsite checkout applications.
create or replace table `etsy-data-warehouse-prod.rollups.agentic_offsite_listing_gms` as 
with tran as (
  -- Aggregate transaction value, units, and review activity to receipt/listing.
  select 
    b.receipt_id,
    b.listing_id,
    count(distinct b.transaction_id) total_units, 
    sum(c.gms_gross) gms_gross, 
    sum(c.gms_net) gms_net, 
    count(case when tr.rating is not null then b.transaction_id end) transaction_w_rating,
    count(tr.rating) total_ratings,
    sum(tr.rating) sum_ratings
  from etsy-data-warehouse-prod.transaction_mart.all_transactions as b 
  left join etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans as c on b.transaction_id = c.transaction_id
  left join etsy-data-warehouse-prod.rollups.transaction_reviews as tr on b.transaction_id = tr.transaction_id
  where b.date >= "2025-09-29"
  group by all
), 
vintage as ( 
  -- Extract the latest "when made" listing property used to identify vintage items.
  select  
    listing_id, 
    case 
      when max(case when property_id = 175264397 then value end) like '%made%' then max(case when property_id = 175264397 then value end) 
      when length(max(case when property_id = 175264397 then value end)) > 1 then SPLIT(max(case when property_id = 175264397 then value end), ',')[OFFSET(1)]  end as WhenMade
  from 
  (select  
      lp.listings_property_id, 
      lp.listing_id , 
      lp.shop_id , 
      lp.property_id , 
      lp.value , 
      lp.created ,
      lp.modified,
      p.name
      from `etsy-data-warehouse-prod.etsy_shard.listings_properties_values` as lp
      left join `etsy-data-warehouse-prod.etsy_index.properties`  as p  on lp.property_id = p.property_id
      where lp.property_id = 175264397
      qualify row_number() over (partition by lp.listing_id order by coalesce(lp.modified, lp.created) desc) = 1
  )
  group by all
), 
him as ( select 
    -- Translate the latest how-it-is-made classification into readable labels.
    -- NOTE: This CTE is currently not referenced in the final SELECT.
    listing_id, 
    case 
          when l1_classification_type = 0 then 'None' 
          when l1_classification_type = 1 then 'SELLER_MADE' 
          when l1_classification_type = 2 then 'SELLER_DESIGNED' 
          when l1_classification_type = 3 then 'SELLER_CURATED' 
          when l1_classification_type = 4 then 'SELLER_SOURCED' 
          end l1_class,
 from etsy-data-warehouse-prod.etsy_shard.listing_how_its_made_classification 
 qualify row_number () over (partition by listing_id order by update_date desc) = 1
)
select 
  date(timestamp_seconds(a.create_date)) as order_date
  , ai.slug application_name
  ,a.multi_shop_cart_id
  ,a.group_payment_id
  ,a.receipt_group_id
  ,r.receipt_id
  ,tran.listing_id
  , case when lower(vin.WhenMade) like '%made%' then 0 when cast(vin.WhenMade as int64) < 2005 then 1 else 0 end is_vintage
  , la.is_personalizable
  , la.is_digital
  ,split(tx.full_path, '.')[SAFE_ORDINAL(1)] AS first_level_category
  ,split(tx.full_path, '.')[SAFE_ORDINAL(2)] AS second_level_category
  ,split(tx.full_path, '.')[SAFE_ORDINAL(3)] AS third_level_category
  ,a.user_id
  ,coalesce(r.mapped_user_id, d.mapped_user_id) mapped_user_id
  , coalesce(mup.buyer_segment, d.buyer_segment) as buyer_segment
  ,d.account_age as mapped_acct_age_days
  ,d.country as mapped_user_country
  ,coalesce(m2.primary_email, m.primary_email) as mapped_user_email
  ,b.seller_user_id
  ,r.is_claimed
  --,b.gms_gross receipt_gms
  --,b.gms_net receipt_gms_net
  --,b.gmv receipt_gmv
  ,tran.total_units
  , tran.gms_gross tran_gms_gross
  , tran.gms_net tran_gms_net
  , tran.transaction_w_rating
  , tran.total_ratings
  , tran.sum_ratings
  from `etsy-data-warehouse-prod.etsy_shard.offsite_checkout_cart` a
  left join `etsy-data-warehouse-prod.transaction_mart.all_receipts` r on r.receipt_group_id = a.receipt_group_id
  left join `etsy-data-warehouse-prod.transaction_mart.receipts_gms` b on r.receipt_id = b.receipt_id
  -- A receipt may contain multiple listings, so this expands the data to one
  -- row per receipt/listing combination.
  left join tran on r.receipt_id = tran.receipt_id
  left join vintage as vin on tran.listing_id = vin.listing_id
  left join `etsy-data-warehouse-prod.user_mart.user_mapping` m on m.user_id = a.user_id
  left join `etsy-data-warehouse-prod.user_mart.user_mapping` m2 on m2.user_id = r.buyer_user_id
  left join `etsy-data-warehouse-prod.user_mart.mapped_user_profile` d on d.mapped_user_id = m.mapped_user_id
  left join  etsy-data-warehouse-prod.materialized.listing_taxonomy as tx on tran.listing_id = tx.listing_id
  left join `etsy-data-warehouse-prod.listing_mart.listing_attributes` as la on la.listing_id = tran.listing_id
  left join `etsy-data-warehouse-prod.etsy_index.application_index` as ai on a.application_id = ai.application_id
  left join mup_mapping mup on mup.user_id = coalesce(r.buyer_user_id, a.user_id) and mup.year_month = format_timestamp('%Y-%m', timestamp_seconds(a.create_date))
  -- Remove internal, QA, Google/OpenAI, and other known test activity so the
  -- output reflects genuine customer purchases.
  where b.seller_user_id not in (
    select distinct user_id from `etsy-data-warehouse-prod.etsy_shard.test_users`
  )
  and lower(d.primary_email) not like '%openai%'
  and lower(d.primary_email) not in 
          ('XYZ@gmail.com', 'XYZ@etsy.com'
           )
  and lower(d.primary_email) not like '%qa%@gmail.com'
  and lower(d.primary_email) not like '%test%@gmail.com'
  and lower(d.primary_email) not like '%@google.com'
  and lower(d.primary_email) not in (select lower(primary_email_id) from `etsy-data-warehouse-prod.static.agentic_test_accounts`)
  ;

--------- AGENTIC REFERRAL PURCHASE ENRICHMENT

-- Enrich attributed agentic-referral purchases with buyer, listing, GMS,
-- review, and channel dimensions at the receipt/listing level.
create or replace table `etsy-data-warehouse-prod.rollups.agentic_referral_listing_gms2` as (
with tran_m as (
  -- Aggregate transaction and review measures while retaining buyer identity.
  select 
    b.receipt_id,
    b.listing_id,
    b.buyer_user_id,
    b.mapped_user_id, 
    count(distinct b.transaction_id) total_units, 
    sum(c.gms_gross) gms_gross, 
    sum(c.gms_net) gms_net, 
    count(case when tr.rating is not null then b.transaction_id end) transaction_w_rating,
    count(tr.rating) total_ratings,
    sum(tr.rating) sum_ratings
  from etsy-data-warehouse-prod.transaction_mart.all_transactions as b 
  left join etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans as c on b.transaction_id = c.transaction_id
  left join etsy-data-warehouse-prod.rollups.transaction_reviews as tr on b.transaction_id = tr.transaction_id
  where b.date >= "2024-01-01"
  group by all
), 
vintage as ( 
  -- Extract the latest "when made" value for vintage classification.
  select  
    listing_id, 
    case 
      when max(case when property_id = 175264397 then value end) like '%made%' then max(case when property_id = 175264397 then value end) 
      when length(max(case when property_id = 175264397 then value end)) > 1 then SPLIT(max(case when property_id = 175264397 then value end), ',')[OFFSET(1)]  end as WhenMade
  from 
  (select  
      lp.listings_property_id, 
      lp.listing_id , 
      lp.shop_id , 
      lp.property_id , 
      lp.value , 
      lp.created ,
      lp.modified,
      p.name
      from `etsy-data-warehouse-prod.etsy_shard.listings_properties_values` as lp
      left join `etsy-data-warehouse-prod.etsy_index.properties`  as p  on lp.property_id = p.property_id
      where lp.property_id = 175264397
      qualify row_number() over (partition by lp.listing_id order by coalesce(lp.modified, lp.created) desc) = 1
  )
  group by all
), 
him as ( select 
    -- Translate the latest how-it-is-made classification into readable labels.
    -- NOTE: This CTE is currently not referenced in the final SELECT.
    listing_id, 
    case 
          when l1_classification_type = 0 then 'None' 
          when l1_classification_type = 1 then 'SELLER_MADE' 
          when l1_classification_type = 2 then 'SELLER_DESIGNED' 
          when l1_classification_type = 3 then 'SELLER_CURATED' 
          when l1_classification_type = 4 then 'SELLER_SOURCED' 
          end l1_class,
 from etsy-data-warehouse-prod.etsy_shard.listing_how_its_made_classification 
 qualify row_number () over (partition by listing_id order by update_date desc) = 1
)
SELECT
  rai.ai_agent,
  rai.is_chatgpt_app,
  rai.conversation_intent,
  rai.channel_group,
  rai.second_channel,
  rai.detected_region,
  rai.visit_date
  ,rai.receipt_id
  ,rai.tran_listing_id
  , case when lower(vin.WhenMade) like '%made%' then 0 when cast(vin.WhenMade as int64) < 2005 then 1 else 0 end is_vintage
  , la.is_personalizable
  , la.is_digital
  ,rai.first_level_category
  ,rai.second_level_category
  ,rai.third_level_category
  ,tran_m.buyer_user_id user_id
  ,rai.mapped_user_id
  ,d.account_age as mapped_acct_age_days
  ,d.country as mapped_user_country
  ,d.primary_email as mapped_user_email
  , coalesce(mup.buyer_segment, d.buyer_segment) as buyer_segment
  ,b.seller_user_id
  --,b.gms_gross receipt_gms
  --,b.gms_net receipt_gms_net
  --,b.gmv receipt_gmv
  ,rai.total_unit_trn
  , rai.gms_gross_trn tran_gms_gross
  , rai.gms_net_trn tran_gms_net
  , tran_m.transaction_w_rating
  , tran_m.total_ratings
  , tran_m.sum_ratings
  , rai.attributed_gms 
  , rai.attributed_receipts
FROM
`etsy-data-warehouse-prod.rollups.agentic_referral_listing_gms` as rai
left join `etsy-data-warehouse-prod.transaction_mart.receipts_gms` b on rai.receipt_id = b.receipt_id
left join `etsy-data-warehouse-prod.user_mart.mapped_user_profile` d on d.mapped_user_id = rai.mapped_user_id
left join tran_m on rai.receipt_id = tran_m.receipt_id and rai.tran_listing_id = tran_m.listing_id
left join vintage as vin on rai.tran_listing_id = vin.listing_id
left join `etsy-data-warehouse-prod.listing_mart.listing_attributes` as la on la.listing_id = rai.tran_listing_id
left join mup_mapping mup on mup.user_id = coalesce(tran_m.buyer_user_id, rai.user_id) and mup.year_month = format_timestamp('%Y-%m', rai.visit_date)
-- Retain only referral visits that can be tied to a completed receipt.
where rai.receipt_id is not null
);


----- COMBINED CHANNEL REPORTING




-- Create a common aggregated schema for comparing offsite checkout, agentic
-- referral, PLA, and Etsy-wide purchase performance.
-- CAUTION: PLA and Overall Etsy are comparison groups that overlap with other
-- rows; categories are not mutually exclusive and should not be summed.
create or replace table `etsy-data-warehouse-prod.rollups.all_agentic_purchases_w_referral` as (
-- Offsite checkout performance, separated by the originating application.
select 
  concat('Offsite checkout - ', a.application_name) as category
  ,a.order_date
  ,count(distinct a.receipt_id) total_receipts
  ,count(distinct a.listing_id) total_listings
  ,a.is_vintage
  ,a.is_personalizable
  ,a.is_digital
  ,a.first_level_category
  ,a.second_level_category
  ,a.third_level_category
  ,a.buyer_segment
  ,count(distinct a.user_id) total_users
  ,count(distinct a.mapped_user_id) total_mapped_users
  --,account_age as mapped_acct_age_days
  ,a.mapped_user_country
  --,primary_email as mapped_user_email
  --,seller_user_id
  ,a.is_claimed
  ,sum(a.total_units) total_units
  ,sum(a.tran_gms_gross)tran_gms_gross
  ,sum(a.tran_gms_net) tran_gms_net
  ,sum(a.transaction_w_rating) transaction_w_rating
  ,sum(a.total_ratings) total_ratings
  ,sum(a.sum_ratings) sum_ratings
  ,cast(null as float64) as attributed_gms
  ,cast(null as float64) as attributed_receipts
from `etsy-data-warehouse-prod.rollups.agentic_offsite_listing_gms`  as a
group by all
union all
-- Agentic-referral performance, separated by AI agent. ChatGPT app traffic is
-- intentionally split from other ChatGPT referral traffic.
select
  -- ChatGPT-app traffic split out into its own bucket; everything else groups
  -- under the ai_agent name (e.g. 'Agentic referral - chatgpt' for chatgpt.com
  -- organic, 'Agentic referral - perplexity', etc.).
  concat('Agentic referral - ',
         case when is_chatgpt_app then 'chatgpt_app' else ai_agent end) as category
  , visit_date
  , count(distinct receipt_id) total_receipt
  ,count(distinct tran_listing_id) total_listings
  , is_vintage
  , is_personalizable
  , is_digital
  ,first_level_category
  ,second_level_category
  ,third_level_category
  , buyer_segment
  ,count(distinct user_id) total_users
  ,count(distinct mapped_user_id) total_mapped_users
  --,d.account_age as mapped_acct_age_days
  ,mapped_user_country
  --, null as mapped_user_email --doesnt matter
  --,b.seller_user_id
  , cast(null as int64) as is_claimed
  ,sum(total_unit_trn) total_unit_trn
  , sum(tran_gms_gross) tran_gms_gross 
  , sum(tran_gms_net)  tran_gms_net
  , sum(transaction_w_rating) transaction_w_rating
  , sum(total_ratings) total_ratings
  , sum(sum_ratings) sum_ratings
  , sum(attributed_gms) attributed_gms
  , sum(attributed_receipts) attributed_receipts
from `etsy-data-warehouse-prod.rollups.agentic_referral_listing_gms2`
where visit_date >= '2025-07-10'  and ai_agent <> 'unknown'
group by all
union all 
-- Google Product Listing Ads provide a paid-shopping comparison benchmark.
select 
  'google pla' as category
  , visit_date
  , count(distinct receipt_id) total_receipt
  ,count(distinct tran_listing_id) total_listings
  , is_vintage
  , is_personalizable
  , is_digital
  ,first_level_category
  ,second_level_category
  ,third_level_category
  , buyer_segment
  ,count(distinct user_id) total_users
  ,count(distinct mapped_user_id) total_mapped_users
  --,d.account_age as mapped_acct_age_days
  ,mapped_user_country
  --, null as mapped_user_email --doesnt matter
  --,b.seller_user_id
  , cast(null as int64) as is_claimed
  ,sum(total_unit_trn) total_unit_trn
  , sum(tran_gms_gross) tran_gms_gross 
  , sum(tran_gms_net)  tran_gms_net
  , sum(transaction_w_rating) transaction_w_rating
  , sum(total_ratings) total_ratings
  , sum(sum_ratings) sum_ratings
  , sum(attributed_gms) attributed_gms
  , sum(attributed_receipts) attributed_receipts
from `etsy-data-warehouse-prod.rollups.agentic_referral_listing_gms2`
where visit_date >= '2025-07-10' and channel_group = 'PLAs' and second_channel = "gpla"
group by all
union all 
-- All PLA traffic provides the broader paid-listing comparison benchmark.
select 
  'PLA' as category
  , visit_date
  , count(distinct receipt_id) total_receipt
  ,count(distinct tran_listing_id) total_listings
  , is_vintage
  , is_personalizable
  , is_digital
  ,first_level_category
  ,second_level_category
  ,third_level_category
  , buyer_segment
  ,count(distinct user_id) total_users
  ,count(distinct mapped_user_id) total_mapped_users
  --,d.account_age as mapped_acct_age_days
  ,mapped_user_country
  --, null as mapped_user_email --doesnt matter
  --,b.seller_user_id
  , cast(null as int64) as is_claimed
  ,sum(total_unit_trn) total_unit_trn
  , sum(tran_gms_gross) tran_gms_gross 
  , sum(tran_gms_net)  tran_gms_net
  , sum(transaction_w_rating) transaction_w_rating
  , sum(total_ratings) total_ratings
  , sum(sum_ratings) sum_ratings
  , sum(attributed_gms) attributed_gms
  , sum(attributed_receipts) attributed_receipts
from `etsy-data-warehouse-prod.rollups.agentic_referral_listing_gms2`
where visit_date >= '2025-07-10' and channel_group = 'PLAs'
group by all
union all 
-- Overall Etsy purchases provide a marketplace-wide comparison benchmark.
select 
  'Overall etsy' as category
  , visit_date
  , count(distinct receipt_id) total_receipt
  ,count(distinct tran_listing_id) total_listings
  , is_vintage
  , is_personalizable
  , is_digital
  ,first_level_category
  ,second_level_category
  ,third_level_category
  , buyer_segment
  ,count(distinct user_id) total_users
  ,count(distinct mapped_user_id) total_mapped_users
  --,d.account_age as mapped_acct_age_days
  ,mapped_user_country
  --, null as mapped_user_email --doesnt matter
  --,b.seller_user_id
  , cast(null as int64) as is_claimed
  ,sum(total_unit_trn) total_unit_trn
  , sum(tran_gms_gross) tran_gms_gross 
  , sum(tran_gms_net)  tran_gms_net
  , sum(transaction_w_rating) transaction_w_rating
  , sum(total_ratings) total_ratings
  , sum(sum_ratings) sum_ratings
  , sum(attributed_gms) attributed_gms
  , sum(attributed_receipts) attributed_receipts
from `etsy-data-warehouse-prod.rollups.agentic_referral_listing_gms2`
where visit_date >= '2025-07-10' 
group by all
);


------- BUYER HISTORY AND PURCHASE FREQUENCY

-- Create a receipt-level purchase history used to measure buyer activity before
-- and after the first observed agentic/offsite purchase.
 create or replace table `etsy-data-warehouse-prod.rollups.all_purchases_mapped_user` as 
(select 
  a.mapped_user_id,
  a.buyer_user_id, 
  --case when off.mapped_user_id is not null then 1 else 0 end off_checkout_user, 
  case when op.receipt_group_id is not null then 1 else 0 end off_site_purchase, 
  a.receipt_id, 
  a.receipt_group_id,
  a.creation_tsz, 
  b.gms_gross, 
  b.gms_net
from  `etsy-data-warehouse-prod.transaction_mart.all_receipts` as a 
left join `etsy-data-warehouse-prod.transaction_mart.receipts_gms` b on a.receipt_id = b.receipt_id
left join `etsy-data-warehouse-prod.etsy_shard.offsite_checkout_cart` as op on a.receipt_group_id = op.receipt_group_id
left join 
    (select 
          distinct m.mapped_user_id 
        from `etsy-data-warehouse-prod.etsy_shard.offsite_checkout_cart` as a 
        left join `etsy-data-warehouse-prod.user_mart.user_mapping` m on m.user_id = a.user_id) as off on a.mapped_user_id = off.mapped_user_id
)
;

-- Normalize word order before address comparison. This makes equivalent strings
-- such as "12 MAIN STREET" and "MAIN STREET 12" easier to match.
CREATE TEMP FUNCTION token_sort(str STRING) AS (
  ARRAY_TO_STRING(
    ARRAY(
      SELECT word 
      FROM UNNEST(SPLIT(REGEXP_REPLACE(UPPER(str), r'\s+', ' '), ' ')) AS word 
      WHERE LENGTH(word) > 0
      ORDER BY word
    ),
    ' '
  )
);

-- Convert edit distance into a 0-100 similarity score. Higher values indicate
-- more similar normalized address strings.
CREATE TEMP FUNCTION similarity_score(str1 STRING, str2 STRING) AS (
  CASE 
    WHEN str1 IS NULL OR str2 IS NULL THEN 0
    WHEN LENGTH(str1) = 0 OR LENGTH(str2) = 0 THEN 0
    ELSE 100 - (EDIT_DISTANCE(str1, str2) * 100.0 / GREATEST(LENGTH(str1), LENGTH(str2)))
  END
);


-- Establish one enriched record per offsite purchase and derive known buyer
-- history from mapped-user identity before applying address-based matching.
create temporary table user_agentic_purch_temp as 
(with  first_mapped_user as (
  -- Find each mapped user's earliest observed offsite checkout purchase.
  select 
    coalesce(r.mapped_user_id, m.mapped_user_id) mapped_user_id, 
    min((timestamp_seconds(a.create_date))) first_date
  from `etsy-data-warehouse-prod.etsy_shard.offsite_checkout_cart` as a 
  left join `etsy-data-warehouse-prod.user_mart.user_mapping` m on m.user_id = a.user_id
  left join `etsy-data-warehouse-prod.transaction_mart.all_receipts` r on r.receipt_group_id = a.receipt_group_id
  where date(timestamp_seconds(a.create_date)) >= '2025-09-29'
    and date(timestamp_seconds(a.create_date)) < current_date
    and a.receipt_group_id is not null
   and r.seller_user_id not in 
      (
              select 
                distinct user_id 
              from `etsy-data-warehouse-prod.etsy_shard.test_users`
              )
  group by 1
),
mapped_first_date as (
  -- Find when each mapped-user identity first appeared in the identity graph.
  select 
    mapped_user_id, 
    min(create_date) first_mapped_create
  from `etsy-data-warehouse-prod.user_mart.user_mapping` as a 
  group by 1
), 
etsy_user as (
    -- Identify active registered Etsy accounts, excluding guests and test/admin users.
    select email,
      user_id,
      ui.create_date
      from 
        `etsy-data-warehouse-prod.user_mart.user_email_settings` as ues
        inner join `etsy-data-warehouse-prod.etsy_index.users_index` as ui using (user_id)
        where is_guest = 0 -- Here we only want the actual user_ids, not guests
        and user_state = 'active'
        and is_admin = 0
        and is_test_puppet = 0
)
select 
    date(timestamp_seconds(a.create_date)) as order_date
  , timestamp_seconds(a.create_date) order_timestamp
  , a.create_date timeseconds
  , a.user_id
  , coalesce(r.mapped_user_id, m.mapped_user_id) mapped_user_id
  , case when timestamp_seconds(a.create_date) <= fmu.first_date  then 1 else 0 end first_offsite_purchase_mapped_user
  , fmu.first_date first_mapped_user_offsite_purchase_date
  , case when osp.first_purchase_tsz < fmu.first_date then 1 else 0 end mapped_user_purchased_before
  , osp.first_purchase mapped_user_first_onsite_purchase ---excluding offsite purchase basically
  , date(timestamp_seconds(mf.first_mapped_create)) first_mapped_date
  --, mf.first_mapped_create first_mapped_create_seconds
  , case when gu.guest_user_id is not null then 1 else 0 end guest_user
  , coalesce(mup.buyer_segment, d.buyer_segment) as buyer_segment
  , eu.create_date is not null as is_etsy_user
  , a.group_payment_id
  , a.receipt_group_id
  , r.receipt_id
  , d.account_age as mapped_acct_age_days
  ,d.country as mapped_user_country
  ,coalesce(m2.primary_email, m.primary_email) as mapped_user_email
  ,b.seller_user_id
  ,r.is_claimed
  ,b.gms_gross receipt_gms
  ,b.gms_net receipt_gms_net
  from `etsy-data-warehouse-prod.etsy_shard.offsite_checkout_cart` a
  join `etsy-data-warehouse-prod.user_mart.user_mapping` m on m.user_id = a.user_id
  left join `etsy-data-warehouse-prod.transaction_mart.all_receipts` r on r.receipt_group_id = a.receipt_group_id
  join `etsy-data-warehouse-prod.user_mart.mapped_user_profile` d on d.mapped_user_id = coalesce(r.mapped_user_id, m.mapped_user_id)
  left join `etsy-data-warehouse-prod.user_mart.user_mapping` m2 on m2.user_id = r.buyer_user_id
  left join `etsy-data-warehouse-prod.transaction_mart.receipts_gms` b on r.receipt_id = b.receipt_id
  left join mapped_first_date mf on coalesce(r.mapped_user_id, m.mapped_user_id) = mf.mapped_user_id
  left join first_mapped_user as fmu on coalesce(r.mapped_user_id, m.mapped_user_id) = fmu.mapped_user_id
  left join 
      (select mapped_user_id, date(min(creation_tsz)) first_purchase, min(creation_tsz) first_purchase_tsz
      from `etsy-data-warehouse-prod.rollups.all_purchases_mapped_user` 
      where off_site_purchase = 0 group by 1) as osp on m.mapped_user_id = osp.mapped_user_id
  left join `etsy-data-warehouse-prod.etsy_index.guest_users_index` as gu on a.user_id = gu.guest_user_id
  left join etsy_user as eu on d.primary_email = eu.email 
  left join mup_mapping mup on mup.user_id = coalesce(r.buyer_user_id, a.user_id) and mup.year_month = format_timestamp('%Y-%m', timestamp_seconds(a.create_date))
  where 
       a.receipt_group_id is not null 
  and date(timestamp_seconds(a.create_date)) >= '2025-09-29'
  and date(timestamp_seconds(a.create_date)) < current_date
  and b.seller_user_id 
        not in (
              select 
                distinct user_id 
              from `etsy-data-warehouse-prod.etsy_shard.test_users`
              )
  and lower(d.primary_email) not like '%openai%'
  and lower(d.primary_email) not in 
          ('XYZ@etsy.com'
           )
  and lower(d.primary_email) not like '%qa%@gmail.com'
  and lower(d.primary_email) not like '%test%@gmail.com'
  and lower(d.primary_email) not like '%@google.com'
  and lower(d.primary_email) not in (select lower(primary_email_id) from `etsy-data-warehouse-prod.static.agentic_test_accounts`)
)
;


-- Supplement mapped-user history with address-based identity resolution. This
-- helps recognize returning Etsy buyers whose offsite purchase was not linked
-- to their historical mapped-user identity.
CREATE OR REPLACE TABLE `etsy-data-warehouse-prod.rollups.user_agentic_purch` AS (
WITH agentic_purch AS (
  -- Attach shipping-address fields to each agentic/offsite purchase and create
  -- a normalized full-address string for matching.
  SELECT 
    ai.*,
    sr.name AS agentic_name,
    sr.first_line AS agentic_first_line, 
    sr.second_line AS agentic_second_line,
    sr.city AS agentic_city, 
    sr.state AS agentic_state,
    sr.zip AS agentic_zip, 
    sr.country_id AS agentic_country_id,
    
    -- Normalize FULL address for matching (including name)
    UPPER(TRIM(REGEXP_REPLACE(CONCAT(
      COALESCE(sr.name, ''), ' ',
      COALESCE(sr.first_line, ''), ' ',
      COALESCE(sr.second_line, ''), ' ',
      COALESCE(sr.city, ''), ' ',
      COALESCE(sr.state, ''), ' ',
      COALESCE(sr.zip, '')
    ), r'\s+', ' '))) as agentic_normalized_full_address
    
  FROM user_agentic_purch_temp AS ai
  LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_receipts2` AS sr 
    ON ai.receipt_id = sr.receipt_id
),

all_offsite_purch AS (
  -- Build the comparable address history for all Etsy receipts and flag whether
  -- each receipt came through offsite checkout.
  SELECT 
    a.mapped_user_id,
    a.buyer_user_id, 
    a.creation_tsz,
    CASE WHEN op.receipt_group_id IS NOT NULL THEN 1 ELSE 0 END AS off_site_purchase, 
    a.receipt_id, 
    a.receipt_group_id,
    sr.name,
    sr.first_line, 
    sr.second_line,
    sr.city, 
    sr.state,
    sr.zip, 
    sr.country_id,
    
    -- Normalize FULL address for matching (including name)
    UPPER(TRIM(REGEXP_REPLACE(CONCAT(
      COALESCE(sr.name, ''), ' ',
      COALESCE(sr.first_line, ''), ' ',
      COALESCE(sr.second_line, ''), ' ',
      COALESCE(sr.city, ''), ' ',
      COALESCE(sr.state, ''), ' ',
      COALESCE(sr.zip, '')
    ), r'\s+', ' '))) as normalized_full_address
    
  FROM `etsy-data-warehouse-prod.transaction_mart.all_receipts` AS a 
  LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.receipts_gms` b 
    ON a.receipt_id = b.receipt_id
  LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.offsite_checkout_cart` AS op 
    ON a.receipt_group_id = op.receipt_group_id
  LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_receipts2` AS sr 
    ON a.receipt_id = sr.receipt_id
  WHERE 
    a.receipt_live = 1  -- Only live receipts
    AND a.creation_tsz IS NOT NULL
),

-- Filter to only ONSITE historical purchases
historical_onsite_purchases AS (
  -- Candidate history is restricted to ordinary onsite Etsy purchases.
  SELECT *
  FROM all_offsite_purch
  WHERE off_site_purchase = 0  -- Only ONSITE purchases
),

matches AS (
  -- Generate plausible historical matches within the same country/state/ZIP,
  -- then score exact and fuzzy address agreement.
  SELECT 
    a.receipt_id AS agentic_receipt_id,

    h.receipt_id AS historical_receipt_id,
    h.creation_tsz AS historical_purchase_timestamp,
    h.buyer_user_id AS historical_buyer_user_id,
    h.mapped_user_id AS historical_mapped_user_id,
    h.name AS historical_name,
    h.first_line AS historical_first_line,
    h.second_line AS historical_second_line,
    h.city AS historical_city,
    h.state AS historical_state,
    h.zip AS historical_zip,
    h.country_id AS historical_country_id,
    h.normalized_full_address AS historical_normalized_full_address,
    
    -- Match Strategy 1: Exact FULL match (name + street + city + state + zip)
    CASE 
      WHEN a.agentic_zip = h.zip 
           AND UPPER(TRIM(a.agentic_name)) = UPPER(TRIM(h.name))
           AND UPPER(TRIM(a.agentic_first_line)) = UPPER(TRIM(h.first_line))
           AND UPPER(TRIM(a.agentic_city)) = UPPER(TRIM(h.city))
           AND UPPER(TRIM(a.agentic_state)) = UPPER(TRIM(h.state))
      THEN 100
      ELSE 0 
    END AS exact_match_score,
    
    -- Match Strategy 2: Token sort fuzzy match on FULL address (including name and street)
    similarity_score(
      token_sort(a.agentic_normalized_full_address),
      token_sort(h.normalized_full_address)
    ) AS fuzzy_match_score,
    
    -- Combined score (best of the two strategies)
    GREATEST(
      CASE 
        WHEN a.agentic_zip = h.zip 
             AND UPPER(TRIM(a.agentic_name)) = UPPER(TRIM(h.name))
             AND UPPER(TRIM(a.agentic_first_line)) = UPPER(TRIM(h.first_line))
             AND UPPER(TRIM(a.agentic_city)) = UPPER(TRIM(h.city))
             AND UPPER(TRIM(a.agentic_state)) = UPPER(TRIM(h.state))
        THEN 100
        ELSE 0 
      END,
      similarity_score(
        token_sort(a.agentic_normalized_full_address),
        token_sort(h.normalized_full_address)
      )
    ) AS combined_score,
    
    -- Rank candidates by match quality, using the earliest purchase to break
    -- ties among candidates with the same best score.
    ROW_NUMBER() OVER (
      PARTITION BY a.receipt_id 
      ORDER BY 
        -- 1st priority: Highest combined score
        GREATEST(
          CASE 
            WHEN a.agentic_zip = h.zip 
                 AND UPPER(TRIM(a.agentic_name)) = UPPER(TRIM(h.name))
                 AND UPPER(TRIM(a.agentic_first_line)) = UPPER(TRIM(h.first_line))
                 AND UPPER(TRIM(a.agentic_city)) = UPPER(TRIM(h.city))
                 AND UPPER(TRIM(a.agentic_state)) = UPPER(TRIM(h.state))
            THEN 100
            ELSE 0 
          END,
          similarity_score(
            token_sort(a.agentic_normalized_full_address),
            token_sort(h.normalized_full_address)
          )
        ) DESC,
        -- 2nd priority: Prefer exact match over fuzzy
        CASE 
          WHEN a.agentic_zip = h.zip 
               AND UPPER(TRIM(a.agentic_name)) = UPPER(TRIM(h.name))
               AND UPPER(TRIM(a.agentic_first_line)) = UPPER(TRIM(h.first_line))
               AND UPPER(TRIM(a.agentic_city)) = UPPER(TRIM(h.city))
               AND UPPER(TRIM(a.agentic_state)) = UPPER(TRIM(h.state))
          THEN 100
          ELSE 0 
        END DESC,
        -- 3rd priority: Earliest purchase wins
        h.creation_tsz ASC
    ) AS earliest_match_rank,
    
    -- Create a second ranking that uses the latest purchase to break ties among
    -- candidates with the same best score.
    ROW_NUMBER() OVER (
      PARTITION BY a.receipt_id 
      ORDER BY 
        -- 1st priority: Highest combined score
        GREATEST(
          CASE 
            WHEN a.agentic_zip = h.zip 
                 AND UPPER(TRIM(a.agentic_name)) = UPPER(TRIM(h.name))
                 AND UPPER(TRIM(a.agentic_first_line)) = UPPER(TRIM(h.first_line))
                 AND UPPER(TRIM(a.agentic_city)) = UPPER(TRIM(h.city))
                 AND UPPER(TRIM(a.agentic_state)) = UPPER(TRIM(h.state))
            THEN 100
            ELSE 0 
          END,
          similarity_score(
            token_sort(a.agentic_normalized_full_address),
            token_sort(h.normalized_full_address)
          )
        ) DESC,
        -- 2nd priority: Prefer exact match over fuzzy
        CASE 
          WHEN a.agentic_zip = h.zip 
               AND UPPER(TRIM(a.agentic_name)) = UPPER(TRIM(h.name))
               AND UPPER(TRIM(a.agentic_first_line)) = UPPER(TRIM(h.first_line))
               AND UPPER(TRIM(a.agentic_city)) = UPPER(TRIM(h.city))
               AND UPPER(TRIM(a.agentic_state)) = UPPER(TRIM(h.state))
          THEN 100
          ELSE 0 
        END DESC,
        -- 3rd priority: LATEST purchase wins (DESC instead of ASC)
        h.creation_tsz DESC
    ) AS latest_match_rank
    
  FROM agentic_purch a
  INNER JOIN historical_onsite_purchases h
    ON a.agentic_country_id = h.country_id
    AND a.agentic_state = h.state
    AND a.agentic_zip = h.zip
    AND h.creation_tsz < a.order_timestamp
    AND h.receipt_id != a.receipt_id
),

-- Retain the highest-quality candidate, preferring the earliest date on ties.
best_earliest_matches AS (
  SELECT *
  FROM matches
  WHERE earliest_match_rank = 1
),

-- Retain the highest-quality candidate, preferring the latest date on ties.
best_latest_matches AS (
  SELECT *
  FROM matches
  WHERE latest_match_rank = 1
),

-- Summarize all high-confidence candidates (combined score >= 90) for each
-- agentic receipt.
match_stats AS (
  SELECT
    agentic_receipt_id,
    COUNT(*) AS num_potential_matches,
    MIN(historical_purchase_timestamp) AS earliest_matched_purchase_date,
    MAX(historical_purchase_timestamp) AS latest_matched_purchase_date
  FROM matches
  WHERE combined_score >= 90  -- Only count decent matches
  GROUP BY agentic_receipt_id
)

-- Final buyer-level output combines identity-based and address-based evidence.
-- A buyer is considered existing when mapped history exists or a high-confidence
-- address match (score >= 90) is found.
SELECT 
  -- ALL ORIGINAL COLUMNS from agentic_purch CTE (already includes address columns)
  a.order_date
  , a.order_timestamp
  , a.timeseconds
  , a.user_id
  , a.mapped_user_id
  , a.first_offsite_purchase_mapped_user
  , a.first_mapped_user_offsite_purchase_date
  , a.mapped_user_purchased_before
  , a.mapped_user_first_onsite_purchase
  , LEAST( COALESCE(mapped_user_first_onsite_purchase, DATE '9999-12-31'),COALESCE(DATE(earliest_matched_purchase_date), DATE '9999-12-31')) first_onsite_purchase ---excluding offsite purchase basically
  , case when (mapped_user_purchased_before = 1 or COALESCE(bm_late.combined_score, 0) >= 90) then 1 else 0 end as already_etsy_buyer
  , a.first_mapped_date
  --, mf.first_mapped_create first_mapped_create_seconds
  , a.guest_user
  , a.buyer_segment
  , a.is_etsy_user
  , a.group_payment_id
  , a.receipt_group_id
  , a.receipt_id
  , a.mapped_acct_age_days
  , a.mapped_user_country
  , a.mapped_user_email
  , a.seller_user_id
  , a.is_claimed
  , a.receipt_gms
  , a.receipt_gms_net
  
  -- THREE SEPARATE MATCH SCORES (COALESCED to 0 if no match)
  -- These come from the EARLIEST match
  ,COALESCE(bm_early.exact_match_score, 0) AS exact_match_score
  ,COALESCE(bm_early.fuzzy_match_score, 0) AS fuzzy_match_score
  ,COALESCE(bm_early.combined_score, 0) AS early_combined_score
  ,COALESCE(bm_late.combined_score, 0) AS late_combined_score
  
  -- How many potential matches were found
  ,COALESCE(ms.num_potential_matches, 0) AS num_potential_address_matches
  
  -- EARLIEST BEST MATCH DETAILS
  ,bm_early.historical_receipt_id AS earliest_matched_receipt_id
  ,bm_early.historical_purchase_timestamp AS earliest_matched_purchase_date
  ,bm_early.historical_buyer_user_id AS earliest_matched_buyer_user_id
  ,bm_early.historical_mapped_user_id AS earliest_matched_mapped_user_id
  ,bm_early.historical_name AS earliest_matched_address_name
  ,bm_early.historical_first_line AS earliest_matched_address_first_line
  ,bm_early.historical_second_line AS earliest_matched_address_second_line
  ,bm_early.historical_city AS earliest_matched_address_city
  ,bm_early.historical_state AS earliest_matched_address_state
  ,bm_early.historical_zip AS earliest_matched_address_zip
  ,bm_early.historical_country_id as earliest_matched_country_id
  
  -- LATEST BEST MATCH DETAILS (NEW!)
  ,bm_late.historical_receipt_id AS latest_matched_receipt_id
  ,bm_late.historical_purchase_timestamp AS latest_matched_purchase_date
  ,bm_late.historical_buyer_user_id AS latest_matched_buyer_user_id
  ,bm_late.historical_mapped_user_id AS latest_matched_mapped_user_id
  ,bm_late.historical_name AS latest_matched_address_name
  ,bm_late.historical_first_line AS latest_matched_address_first_line
  ,bm_late.historical_second_line AS latest_matched_address_second_line
  ,bm_late.historical_city AS latest_matched_address_city
  ,bm_late.historical_state AS latest_matched_address_state
  ,bm_late.historical_zip AS latest_matched_address_zip
  ,bm_late.historical_country_id as latest_matched_country_id
  -- AGGREGATE DATES (from match_stats)
  ,ms.earliest_matched_purchase_date AS earliest_address_onsite_purchase_date
  ,ms.latest_matched_purchase_date AS latest_address_onsite_purchase_date
  
  -- Time metrics
  ,DATE_DIFF(
    DATE(a.order_timestamp),
    DATE(bm_early.historical_purchase_timestamp),
    DAY
  ) AS days_since_earliest_best_match
  
  ,DATE_DIFF(
    DATE(a.order_timestamp),
    DATE(bm_late.historical_purchase_timestamp),
    DAY
  ) AS days_since_latest_best_match
  
  ,DATE_DIFF(
    DATE(a.order_timestamp),
    DATE(ms.earliest_matched_purchase_date),
    DAY
  ) AS days_since_earliest_90_score
  
  ,DATE_DIFF(
    DATE(a.order_timestamp),
    DATE(ms.latest_matched_purchase_date),
    DAY
  ) AS days_since_latest_90_score
  
  -- Address + Name comparison for manual review (from earliest match)
  ,CASE 
    WHEN bm_early.historical_first_line IS NOT NULL THEN
      CONCAT(
        'Name: [', COALESCE(a.agentic_name, 'NULL'), ' vs ', COALESCE(bm_early.historical_name, 'NULL'), '] | ',
        'Address: [', COALESCE(a.agentic_first_line, 'NULL'), ' vs ', COALESCE(bm_early.historical_first_line, 'NULL'), ']'
      )
    ELSE NULL
  END AS best_early_address_comp

  ,CASE 
    WHEN bm_late.historical_first_line IS NOT NULL THEN
      CONCAT(
        'Name: [', COALESCE(a.agentic_name, 'NULL'), ' vs ', COALESCE(bm_late.historical_name, 'NULL'), '] | ',
        'Address: [', COALESCE(a.agentic_first_line, 'NULL'), ' vs ', COALESCE(bm_late.historical_first_line, 'NULL'), ']'
      )
    ELSE NULL
  END AS best_latest_address_comp

FROM agentic_purch a
LEFT JOIN best_earliest_matches bm_early
  ON a.receipt_id = bm_early.agentic_receipt_id
LEFT JOIN best_latest_matches bm_late
  ON a.receipt_id = bm_late.agentic_receipt_id
LEFT JOIN match_stats ms
  ON a.receipt_id = ms.agentic_receipt_id
ORDER BY a.order_date DESC
);

--- OFFSITE BUYER RETENTION AND REPEAT-PURCHASE MEASUREMENT

-- For each mapped user's first offsite purchase, measure prior-year purchase
-- frequency and subsequent onsite activity over fixed maturity windows.
-- Each metric is populated only after the complete observation window has elapsed.
create or replace table `etsy-data-warehouse-prod.rollups.offsite_purch_freq` as
select 
      first_offsite_purchase,
      already_etsy_buyer,
      customer_age_category, 
      case when orders_before_offsite_365 > 100 then 'a.more than 100 orders'
            when orders_before_offsite_365 > 50 then 'b. 50 - 100 orders'
            when orders_before_offsite_365 > 25 then 'c. 26 - 50 orders'
            when orders_before_offsite_365 > 10 then 'd. 11 - 25 orders'
            when orders_before_offsite_365 > 5 then 'e. 6 - 10 orders'
            when orders_before_offsite_365 > 0 then 'f. 1 - 5 orders'
            when orders_before_offsite_365 = 0 then 'g. No orders'
            end orders_before_offsite_365_cat, ---this is 365 days before their first purchase on offsite
      mapped_user_id,
      sum(orders_before_offsite_365) orders_before_offsite_365, 
      sum(users_purch_14dlater_on_etsy) users_purch_14dlater_on_etsy,
      sum(orders_purch_14dlater_on_etsy) orders_purch_14dlater_on_etsy,
      sum(gms_14dlater_on_etsy) gms_14dlater_on_etsy,
      sum(users_purch_30dlater_on_etsy) users_purch_30dlater_on_etsy,
      sum(orders_purch_30dlater_on_etsy) orders_purch_30dlater_on_etsy,
      sum(gms_30dlater_on_etsy) gms_30dlater_on_etsy, 
      sum(users_purch_3mlater_on_etsy) users_purch_3mlater_on_etsy, 
      sum(orders_purch_3mlater_on_etsy) orders_purch_3mlater_on_etsy, 
      sum(gms_3mlater_on_etsy) gms_3mlater_on_etsy, 

      sum(users_purch_6mlater_on_etsy) users_purch_6mlater_on_etsy, 
      sum(orders_purch_6mlater_on_etsy) orders_purch_6mlater_on_etsy, 
      sum(gms_6mlater_on_etsy) gms_6mlater_on_etsy, 

      sum(users_purch_12mlater_on_etsy) users_purch_12mlater_on_etsy, 
      sum(orders_purch_12mlater_on_etsy) orders_purch_12mlater_on_etsy, 
      sum(gms_12mlater_on_etsy) gms_12mlater_on_etsy, 

    from
          (select 
            a.mapped_user_id,
            first_offsite_purchase,
            case when user_age_with_etsy >= 365 then 'a. more than 1 year old etsy buyer'
                when user_age_with_etsy >= 180 then 'b. 6 month - 1 year old etsy buyer'
                when user_age_with_etsy >= 90 then 'c. 90 - 6 month old etsy buyer'
                when user_age_with_etsy >= 60 then 'd. 60 - 90 day old etsy buyer'
                when user_age_with_etsy >= 30 then 'e. 30 - 60 day old etsy buyer'
                when user_age_with_etsy >= 15 then 'f. 15 - 30 day old etsy buyer'
                when user_age_with_etsy > 0 then 'g. 0 - 15 day old etsy buyer'
                when user_age_with_etsy = 0 then 'h. purchased on etsy same day of offsite'
                else 'i. no purchase on etsy before'
                end customer_age_category, 
            already_etsy_buyer, 
            sum(coalesce(receipt_gms, 0)) first_purchase_gms, 
            -- Number of onsite orders during the 365 days preceding the first
            -- offsite purchase.
            count(distinct 
                case when cast(b.creation_tsz as date) < a.first_offsite_purchase 
                and cast(b.creation_tsz as date) >= a.first_offsite_purchase - 365 
                then receipt_id end) orders_before_offsite_365, 
            -- Repeat-buyer, order, and GMS outcomes within 14 days.
            count(distinct 
                      case when first_offsite_purchase < current_date - 14  
                          and cast(b.creation_tsz as date) > a.first_offsite_purchase
                          and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 14 
                          then b.mapped_user_id end) users_purch_14dlater_on_etsy, 
            -- Repeat-buyer, order, and GMS outcomes within 30 days.
            count(distinct 
                      case when first_offsite_purchase < current_date - 14  
                      and cast(b.creation_tsz as date) > a.first_offsite_purchase
                      and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 14 
                      then b.receipt_group_id end ) orders_purch_14dlater_on_etsy,
            sum(case when 
                    first_offsite_purchase < current_date - 14 
                    and cast(b.creation_tsz as date) > a.first_offsite_purchase
                    and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 14 
                    then coalesce(b.gms_gross, 0) end) gms_14dlater_on_etsy, 
            -- Repeat-buyer, order, and GMS outcomes within 90 days.
            count(distinct 
                    case when first_offsite_purchase < current_date - 30 
                    and cast(b.creation_tsz as date) > a.first_offsite_purchase
                    and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 30 
                    then b.mapped_user_id end) users_purch_30dlater_on_etsy, 
            -- Repeat-buyer, order, and GMS outcomes within 180 days.
            count(distinct 
                      case when first_offsite_purchase < current_date - 30 
                      and cast(b.creation_tsz as date) > a.first_offsite_purchase
                      and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 30 
                      then b.receipt_group_id end ) orders_purch_30dlater_on_etsy,
            sum(case when 
                    first_offsite_purchase < current_date - 30 
                    and cast(b.creation_tsz as date) > a.first_offsite_purchase
                    and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 30 
                    then coalesce(b.gms_gross, 0) end) gms_30dlater_on_etsy,

            -- Repeat-buyer, order, and GMS outcomes within 360 days.
            count(distinct 
                    case when first_offsite_purchase < current_date - 90
                    and cast(b.creation_tsz as date) > a.first_offsite_purchase
                    and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 90 
                    then b.mapped_user_id end) users_purch_3mlater_on_etsy, 
            count(distinct 
                      case when first_offsite_purchase < current_date - 90 
                      and cast(b.creation_tsz as date) > a.first_offsite_purchase
                      and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 90 
                      then b.receipt_group_id end ) orders_purch_3mlater_on_etsy,
            sum(case when 
                    first_offsite_purchase < current_date - 90 
                    and cast(b.creation_tsz as date) > a.first_offsite_purchase
                    and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 90 
                    then coalesce(b.gms_gross, 0) end) gms_3mlater_on_etsy,

            count(distinct 
                    case when first_offsite_purchase < current_date - 180
                    and cast(b.creation_tsz as date) > a.first_offsite_purchase
                    and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 180 
                    then b.mapped_user_id end) users_purch_6mlater_on_etsy, 
            count(distinct 
                      case when first_offsite_purchase < current_date - 180 
                      and cast(b.creation_tsz as date) > a.first_offsite_purchase
                      and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 180 
                      then b.receipt_group_id end ) orders_purch_6mlater_on_etsy,
            sum(case when 
                    first_offsite_purchase < current_date - 180 
                    and cast(b.creation_tsz as date) > a.first_offsite_purchase
                    and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 180 
                    then coalesce(b.gms_gross, 0) end) gms_6mlater_on_etsy, 

            count(distinct 
                    case when first_offsite_purchase < current_date - 360
                    and cast(b.creation_tsz as date) > a.first_offsite_purchase
                    and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 360 
                    then b.mapped_user_id end) users_purch_12mlater_on_etsy, 
            count(distinct 
                      case when first_offsite_purchase < current_date - 360 
                      and cast(b.creation_tsz as date) > a.first_offsite_purchase
                      and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 360 
                      then b.receipt_group_id end ) orders_purch_12mlater_on_etsy,
            sum(case when 
                    first_offsite_purchase < current_date - 360 
                    and cast(b.creation_tsz as date) > a.first_offsite_purchase
                    and cast(b.creation_tsz as date) <= a.first_offsite_purchase + 360 
                    then coalesce(b.gms_gross, 0) end) gms_12mlater_on_etsy
          from 
              (select 
                  mapped_user_id, 
                  buyer_segment, 
                  order_date first_offsite_purchase, 
                  first_onsite_purchase,
                  date_diff(order_date, first_onsite_purchase, DAY) user_age_with_etsy,
                  already_etsy_buyer,
                  receipt_gms
              from `etsy-data-warehouse-prod.rollups.user_agentic_purch`
              where 
                  order_date >= '2025-09-29'
                  and order_date < current_date --   --   --14
              qualify row_number() over(partition by mapped_user_id order by order_timestamp asc) = 1 
              ---filtering for first purchase of mapped user but it is possible a lot of these are unmapped known users 
              ---and we also aren't able to track their future purchases on etsy
              ) as a 
          left join `etsy-data-warehouse-prod.rollups.all_purchases_mapped_user` as b 
                on a.mapped_user_id = b.mapped_user_id and b.off_site_purchase = 0
                and  a.first_offsite_purchase - 365 <= cast(b.creation_tsz as date)
          group by all
          )
        group by all
;


-- Add warehouse metadata so downstream users can interpret each retention field.
ALTER TABLE `etsy-data-warehouse-prod.rollups.offsite_purch_freq`
  ALTER COLUMN first_offsite_purchase SET OPTIONS (description = 'first date of offsite agentic purchase'),
  ALTER COLUMN already_etsy_buyer SET OPTIONS (description = 'flag indicating if the mapped user is first time etsy buyer'),
  ALTER COLUMN customer_age_category SET OPTIONS (description = 'age of customer categorized based on their previously matched purchase on etsy excluding offsite'),
  ALTER COLUMN orders_before_offsite_365_cat SET OPTIONS (description = 'Orders before mapped users first purchase category assigned'),
  ALTER COLUMN mapped_user_id SET OPTIONS (description = 'Id of mapped user'), 
  ALTER COLUMN orders_before_offsite_365 SET OPTIONS (description = 'offsite orders 365 purchased before first purchase '), 
  ALTER COLUMN users_purch_14dlater_on_etsy SET OPTIONS (description = '14 day user purch freq after first purchase'),
  ALTER COLUMN orders_purch_14dlater_on_etsy SET OPTIONS (description = '14 day orders purch freq after first purchase'),
  ALTER COLUMN gms_14dlater_on_etsy SET OPTIONS (description = '14 day gms purch freq after first purchase'),
  ALTER COLUMN users_purch_30dlater_on_etsy SET OPTIONS (description = '30 day user purch freq after first purchase'), 
  ALTER COLUMN orders_purch_30dlater_on_etsy SET OPTIONS (description = '30 day order purch freq after first purchase'),
  ALTER COLUMN gms_30dlater_on_etsy SET OPTIONS (description = '30 day gms purch freq after first purchase'), 
  ALTER COLUMN users_purch_3mlater_on_etsy SET OPTIONS (description = '3 month user purch freq after first purchase'), 
  ALTER COLUMN orders_purch_3mlater_on_etsy SET OPTIONS (description = '3 month order purch freq after first purchase'), 
  ALTER COLUMN gms_3mlater_on_etsy SET OPTIONS (description = '3 month gms purch freq after first purchase'), 
  ALTER COLUMN users_purch_6mlater_on_etsy SET OPTIONS (description = '6 month user purch freq after first purchase'), 
  ALTER COLUMN orders_purch_6mlater_on_etsy SET OPTIONS (description = '6 month orders purch freq after first purchase'), 
  ALTER COLUMN gms_6mlater_on_etsy SET OPTIONS (description = '6 month gms purch freq after first purchase'), 
  ALTER COLUMN users_purch_12mlater_on_etsy SET OPTIONS (description = '6 month user purch freq after first purchase'), 
  ALTER COLUMN orders_purch_12mlater_on_etsy SET OPTIONS (description = '6 month order purch freq after first purchase'), 
  ALTER COLUMN gms_12mlater_on_etsy SET OPTIONS (description = '6 month gms purch freq after first purchase')
;


--- AGENTIC-REFERRAL BUYER RETENTION AND REPEAT-PURCHASE MEASUREMENT
-- NOTE: This tracks first purchase in agentic_referral_listing_gms2 table since 2025-01-01
-- NOT necessarily the user's first-ever referral purchase (table contains data from 2024-04-07+)
-- Includes all ai_agent values (including 'unknown')

-- Repeat the maturity-window analysis for purchases attributed to agentic
-- referrals, retaining agent, channel, and conversation-intent dimensions.
create or replace table `etsy-data-warehouse-prod.rollups.agentic_referral_purch_freq` as
select
      ai_agent,
      is_chatgpt_app,
      conversation_intent,
      channel_group,
      second_channel,
      first_referral_purchase,
      already_etsy_buyer,
      customer_age_category,
      case when orders_before_referral_365 > 100 then 'a.more than 100 orders'
            when orders_before_referral_365 > 50 then 'b. 50 - 100 orders'
            when orders_before_referral_365 > 25 then 'c. 26 - 50 orders'
            when orders_before_referral_365 > 10 then 'd. 11 - 25 orders'
            when orders_before_referral_365 > 5 then 'e. 6 - 10 orders'
            when orders_before_referral_365 > 0 then 'f. 1 - 5 orders'
            when orders_before_referral_365 = 0 then 'g. No orders'
            end orders_before_referral_365_cat,
      mapped_user_id,
      sum(orders_before_referral_365) orders_before_referral_365,
      sum(users_purch_14dlater_on_etsy) users_purch_14dlater_on_etsy,
      sum(orders_purch_14dlater_on_etsy) orders_purch_14dlater_on_etsy,
      sum(gms_14dlater_on_etsy) gms_14dlater_on_etsy,
      sum(users_purch_30dlater_on_etsy) users_purch_30dlater_on_etsy,
      sum(orders_purch_30dlater_on_etsy) orders_purch_30dlater_on_etsy,
      sum(gms_30dlater_on_etsy) gms_30dlater_on_etsy,
      sum(users_purch_3mlater_on_etsy) users_purch_3mlater_on_etsy,
      sum(orders_purch_3mlater_on_etsy) orders_purch_3mlater_on_etsy,
      sum(gms_3mlater_on_etsy) gms_3mlater_on_etsy,

      sum(users_purch_6mlater_on_etsy) users_purch_6mlater_on_etsy,
      sum(orders_purch_6mlater_on_etsy) orders_purch_6mlater_on_etsy,
      sum(gms_6mlater_on_etsy) gms_6mlater_on_etsy,

      sum(users_purch_12mlater_on_etsy) users_purch_12mlater_on_etsy,
      sum(orders_purch_12mlater_on_etsy) orders_purch_12mlater_on_etsy,
      sum(gms_12mlater_on_etsy) gms_12mlater_on_etsy

    from
          (select
            a.mapped_user_id,
            ai_agent,
            is_chatgpt_app,
            conversation_intent,
            channel_group,
            second_channel,
            first_referral_purchase,
            first_etsy_purchase,
            case when user_age_with_etsy >= 365 then 'a. more than 1 year old etsy buyer'
                when user_age_with_etsy >= 180 then 'b. 6 month - 1 year old etsy buyer'
                when user_age_with_etsy >= 90 then 'c. 90 - 6 month old etsy buyer'
                when user_age_with_etsy >= 60 then 'd. 60 - 90 day old etsy buyer'
                when user_age_with_etsy >= 30 then 'e. 30 - 60 day old etsy buyer'
                when user_age_with_etsy >= 15 then 'f. 15 - 30 day old etsy buyer'
                when user_age_with_etsy > 0 then 'g. 0 - 15 day old etsy buyer'
                when user_age_with_etsy = 0 then 'h. purchased on etsy same day of referral'
                else 'i. no purchase on etsy before'
                end customer_age_category,
            case when first_etsy_purchase < first_referral_purchase then 1 else 0 end already_etsy_buyer,
            sum(coalesce(tran_gms_gross, 0)) first_purchase_gms,
            count(distinct
                case when cast(b.creation_tsz as date) < a.first_referral_purchase
                and cast(b.creation_tsz as date) >= a.first_referral_purchase - 365
                then receipt_id end) orders_before_referral_365,
            count(distinct
                      case when first_referral_purchase < current_date - 14
                          and cast(b.creation_tsz as date) > a.first_referral_purchase
                          and cast(b.creation_tsz as date) <= a.first_referral_purchase + 14
                          then b.mapped_user_id end) users_purch_14dlater_on_etsy,
            count(distinct
                      case when first_referral_purchase < current_date - 14
                      and cast(b.creation_tsz as date) > a.first_referral_purchase
                      and cast(b.creation_tsz as date) <= a.first_referral_purchase + 14
                      then b.receipt_group_id end ) orders_purch_14dlater_on_etsy,
            sum(case when
                    first_referral_purchase < current_date - 14
                    and cast(b.creation_tsz as date) > a.first_referral_purchase
                    and cast(b.creation_tsz as date) <= a.first_referral_purchase + 14
                    then coalesce(b.gms_gross, 0) end) gms_14dlater_on_etsy,
            count(distinct
                    case when first_referral_purchase < current_date - 30
                    and cast(b.creation_tsz as date) > a.first_referral_purchase
                    and cast(b.creation_tsz as date) <= a.first_referral_purchase + 30
                    then b.mapped_user_id end) users_purch_30dlater_on_etsy,
            count(distinct
                      case when first_referral_purchase < current_date - 30
                      and cast(b.creation_tsz as date) > a.first_referral_purchase
                      and cast(b.creation_tsz as date) <= a.first_referral_purchase + 30
                      then b.receipt_group_id end ) orders_purch_30dlater_on_etsy,
            sum(case when
                    first_referral_purchase < current_date - 30
                    and cast(b.creation_tsz as date) > a.first_referral_purchase
                    and cast(b.creation_tsz as date) <= a.first_referral_purchase + 30
                    then coalesce(b.gms_gross, 0) end) gms_30dlater_on_etsy,

            count(distinct
                    case when first_referral_purchase < current_date - 90
                    and cast(b.creation_tsz as date) > a.first_referral_purchase
                    and cast(b.creation_tsz as date) <= a.first_referral_purchase + 90
                    then b.mapped_user_id end) users_purch_3mlater_on_etsy,
            count(distinct
                      case when first_referral_purchase < current_date - 90
                      and cast(b.creation_tsz as date) > a.first_referral_purchase
                      and cast(b.creation_tsz as date) <= a.first_referral_purchase + 90
                      then b.receipt_group_id end ) orders_purch_3mlater_on_etsy,
            sum(case when
                    first_referral_purchase < current_date - 90
                    and cast(b.creation_tsz as date) > a.first_referral_purchase
                    and cast(b.creation_tsz as date) <= a.first_referral_purchase + 90
                    then coalesce(b.gms_gross, 0) end) gms_3mlater_on_etsy,

            count(distinct
                    case when first_referral_purchase < current_date - 180
                    and cast(b.creation_tsz as date) > a.first_referral_purchase
                    and cast(b.creation_tsz as date) <= a.first_referral_purchase + 180
                    then b.mapped_user_id end) users_purch_6mlater_on_etsy,
            count(distinct
                      case when first_referral_purchase < current_date - 180
                      and cast(b.creation_tsz as date) > a.first_referral_purchase
                      and cast(b.creation_tsz as date) <= a.first_referral_purchase + 180
                      then b.receipt_group_id end ) orders_purch_6mlater_on_etsy,
            sum(case when
                    first_referral_purchase < current_date - 180
                    and cast(b.creation_tsz as date) > a.first_referral_purchase
                    and cast(b.creation_tsz as date) <= a.first_referral_purchase + 180
                    then coalesce(b.gms_gross, 0) end) gms_6mlater_on_etsy,

            count(distinct
                    case when first_referral_purchase < current_date - 360
                    and cast(b.creation_tsz as date) > a.first_referral_purchase
                    and cast(b.creation_tsz as date) <= a.first_referral_purchase + 360
                    then b.mapped_user_id end) users_purch_12mlater_on_etsy,
            count(distinct
                      case when first_referral_purchase < current_date - 360
                      and cast(b.creation_tsz as date) > a.first_referral_purchase
                      and cast(b.creation_tsz as date) <= a.first_referral_purchase + 360
                      then b.receipt_group_id end ) orders_purch_12mlater_on_etsy,
            sum(case when
                    first_referral_purchase < current_date - 360
                    and cast(b.creation_tsz as date) > a.first_referral_purchase
                    and cast(b.creation_tsz as date) <= a.first_referral_purchase + 360
                    then coalesce(b.gms_gross, 0) end) gms_12mlater_on_etsy
          from
              (select
                  ref.mapped_user_id,
                  ref.ai_agent,
                  ref.is_chatgpt_app,
                  ref.conversation_intent,
                  ref.channel_group,
                  ref.second_channel,
                  ref.visit_date as first_referral_purchase,
                  date(min_purch.creation_tsz) as first_etsy_purchase,
                  date_diff(ref.visit_date, date(min_purch.creation_tsz), DAY) user_age_with_etsy,
                  ref.tran_gms_gross
              from
                  (select
                      mapped_user_id,
                      ai_agent,
                      is_chatgpt_app,
                      conversation_intent,
                      channel_group,
                      second_channel,
                      visit_date,
                      tran_gms_gross
                  from `etsy-data-warehouse-prod.rollups.agentic_referral_listing_gms2`
                  where visit_date >= '2025-01-01' ---note this base table for referral is only from 2024 so the first purchases here are based on visits in the table
                    and visit_date < current_date
                    and receipt_id is not null
                  qualify row_number() over(partition by mapped_user_id order by visit_date asc) = 1
                  ) as ref
              left join
                  (select mapped_user_id, min(creation_tsz) creation_tsz
                  from `etsy-data-warehouse-prod.rollups.all_purchases_mapped_user`
                  group by all) as min_purch
                  on ref.mapped_user_id = min_purch.mapped_user_id
              ) as a
          left join `etsy-data-warehouse-prod.rollups.all_purchases_mapped_user` as b
                on a.mapped_user_id = b.mapped_user_id and b.off_site_purchase = 0
                and a.first_referral_purchase - 365 <= cast(b.creation_tsz as date)
          group by all
          )
        group by all
;


-- Add warehouse metadata for the referral retention output.
ALTER TABLE `etsy-data-warehouse-prod.rollups.agentic_referral_purch_freq`
  ALTER COLUMN ai_agent SET OPTIONS (description = 'AI agent that referred the first purchase (e.g., ChatGPT, Gemini, Perplexity)'),
  ALTER COLUMN is_chatgpt_app SET OPTIONS (description = 'TRUE when the first referral purchase came from the ChatGPT plugin app (ref=chatgpt_app in landing URL).'),
  ALTER COLUMN conversation_intent SET OPTIONS (description = 'Conversation primary intent (first display intent, fallback first search) at time of the first referral purchase. NULL for non-chatgpt_app rows.'),
  ALTER COLUMN channel_group SET OPTIONS (description = 'Channel group of the referral (e.g., PLAs, Organic Search)'),
  ALTER COLUMN second_channel SET OPTIONS (description = 'Second level channel detail'),
  ALTER COLUMN first_referral_purchase SET OPTIONS (description = 'first date of agentic referral purchase for mapped user'),
  ALTER COLUMN already_etsy_buyer SET OPTIONS (description = 'flag indicating if the mapped user had purchased on etsy before their first referral purchase (1 = existing buyer, 0 = new buyer)'),
  ALTER COLUMN customer_age_category SET OPTIONS (description = 'age of customer categorized based on their first purchase on etsy excluding referral'),
  ALTER COLUMN orders_before_referral_365_cat SET OPTIONS (description = 'Orders before mapped users first referral purchase category assigned'),
  ALTER COLUMN mapped_user_id SET OPTIONS (description = 'Id of mapped user'),
  ALTER COLUMN orders_before_referral_365 SET OPTIONS (description = 'orders purchased 365 days before first referral purchase'),
  ALTER COLUMN users_purch_14dlater_on_etsy SET OPTIONS (description = '14 day user purch freq after first referral purchase'),
  ALTER COLUMN orders_purch_14dlater_on_etsy SET OPTIONS (description = '14 day orders purch freq after first referral purchase'),
  ALTER COLUMN gms_14dlater_on_etsy SET OPTIONS (description = '14 day gms purch freq after first referral purchase'),
  ALTER COLUMN users_purch_30dlater_on_etsy SET OPTIONS (description = '30 day user purch freq after first referral purchase'),
  ALTER COLUMN orders_purch_30dlater_on_etsy SET OPTIONS (description = '30 day order purch freq after first referral purchase'),
  ALTER COLUMN gms_30dlater_on_etsy SET OPTIONS (description = '30 day gms purch freq after first referral purchase'),
  ALTER COLUMN users_purch_3mlater_on_etsy SET OPTIONS (description = '3 month user purch freq after first referral purchase'),
  ALTER COLUMN orders_purch_3mlater_on_etsy SET OPTIONS (description = '3 month order purch freq after first referral purchase'),
  ALTER COLUMN gms_3mlater_on_etsy SET OPTIONS (description = '3 month gms purch freq after first referral purchase'),
  ALTER COLUMN users_purch_6mlater_on_etsy SET OPTIONS (description = '6 month user purch freq after first referral purchase'),
  ALTER COLUMN orders_purch_6mlater_on_etsy SET OPTIONS (description = '6 month orders purch freq after first referral purchase'),
  ALTER COLUMN gms_6mlater_on_etsy SET OPTIONS (description = '6 month gms purch freq after first referral purchase'),
  ALTER COLUMN users_purch_12mlater_on_etsy SET OPTIONS (description = '12 month user purch freq after first referral purchase'),
  ALTER COLUMN orders_purch_12mlater_on_etsy SET OPTIONS (description = '12 month order purch freq after first referral purchase'),
  ALTER COLUMN gms_12mlater_on_etsy SET OPTIONS (description = '12 month gms purch freq after first referral purchase')
;



END
