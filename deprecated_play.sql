/***************************************************
NOTE: YOU MUST SPECIFY THE CORRECT REPORTING DATE IN THE TABLE XXXXX
****************************************************/
-- load upsells
with current_upsells as (
select * from usertables.mc_current_upsells
),
-- load backlog
backlog_curve as (
select * from usertables.mc_backlog_master_global_csv
),
-- calculat processing volumes before upsells
non_upsell_processing as (
select
'weekly_processing' as data_type,
capture_date,
sales_merchant_id as sales_merchant_id,
'new biz' AS sales_category,
sales_funnel__activation_date as sales_activation_date,
sum(case 
   when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 and upsells.effective_activation_date is null then 1 * ap.npv_usd_fixed / 100 -- no upsell 99.9%
   when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 and datediff('d', upsells.effective_activation_date, capture_date) < 0 then 1 * ap.npv_usd_fixed / 100 -- upsell not started
   when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 and datediff('d', upsells.effective_activation_date, capture_date) >= 0 and datediff('d', upsells.effective_activation_date, capture_date) < 366 then (1 - pct_share_of_npv) * ap.npv_usd_fixed / 100 -- upsell and original sale overlap
   else 0 
end) as first_year_sold_npv_usd_fx,
sum( ap.npv_usd_fixed / 100) as total_npv

from aggregates.payments ap
JOIN dim.merchants AS m ON ap.sales_merchant_id = m._id
LEFT JOIN current_upsells as upsells ON upsells.merchant_id = ap.sales_merchant_id
where
capture_date > '2016-09-30'
and 
m.sales__is_sold = true
and ap.sales_merchant_id in (
'acct_19dnGwDNni28CxKN',
'acct_18PTH9Jfybp8ZNpd',
'acct_185J8ZBqzSuuUrdf',
'acct_187cFEJwYUP6g6g2',
'acct_103UBD2K7Tp1dUfp',
'acct_14vBEUJM1OE6Um6K',
'acct_18S7L7GfxT5XNkxw'
)


group by 1,2,3,4,5),

upsell_processing as (
select
'weekly_processing' as data_type,
capture_date,
sales_merchant_id as sales_merchant_id,
'upsell' AS sales_category,
upsells.effective_activation_date as sales_activation_date,
sum(case 
   when datediff('d', upsells.effective_activation_date, capture_date) >= 0 and datediff('d', upsells.effective_activation_date, capture_date) < 366 then pct_share_of_npv * ap.npv_usd_fixed / 100 -- upsell and original sale overlap
   else 0 
end) as first_year_sold_npv_usd_fx,
sum( ap.npv_usd_fixed / 100) as total_npv

from aggregates.payments ap
JOIN dim.merchants AS m ON ap.sales_merchant_id = m._id
INNER JOIN current_upsells as upsells ON upsells.merchant_id = ap.sales_merchant_id
where
capture_date > '2016-09-30'
and 
m.sales__is_sold = true
and ap.sales_merchant_id in (
'acct_19dnGwDNni28CxKN',
'acct_18PTH9Jfybp8ZNpd',
'acct_185J8ZBqzSuuUrdf',
'acct_187cFEJwYUP6g6g2',
'acct_103UBD2K7Tp1dUfp',
'acct_14vBEUJM1OE6Um6K',
'acct_18S7L7GfxT5XNkxw'
)


group by 1,2,3,4,5), 

processing_volume as (
select * from non_upsell_processing
union
select * from upsell_processing ),

/**********
CHANGE DATES BELOW TO REPORTING DATE (TWO PLACES)
***********/
backlog_summary as (
select
sales_merchant_id,
sales_category, 
sales_activation_date,
datediff('day', pv.sales_activation_date, '2017-02-08') as days_since_activation,      -- UPDATE THIS
first_year_npv,
first_year_npv/first_year_sold_cumulative_pct as first_year_est_npv
from
(select 
sales_merchant_id,
sales_category, 
sales_activation_date,
sum(first_year_sold_npv_usd_fx) as first_year_npv
from processing_volume group by 1,2,3) pv 


inner join backlog_curve bc on bc.days_since_activation = datediff('day', pv.sales_activation_date, '2017-02-08')  -- UPDATE THIS
where first_year_npv > 0)

select
dateadd('day', curve.days_since_activation, sales_activation_date) as fcst_date, 
sales_merchant_id, 
sales_category, 
first_year_est_npv * first_year_sold_pct as backlog_npv
from backlog_summary bs 
cross join backlog_curve as curve
where (curve.days_since_activation between bs.days_since_activation and 365)







