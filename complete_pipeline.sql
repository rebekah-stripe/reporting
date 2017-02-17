

/** for faster access put usertables in memory **/ 
-- backlog
with backlog_curve as (
select * from usertables.mc_backlog_master_global_csv
),
-- country detail mapping
country_code as(
select * from usertables.mc_country_codes_csv),
-- team mapping
team_role as(
select * from usertables.mc_team_role_csv),

/** Opti-calculations **/ 

daily_pipeline as (select  
dateadd(day, curve.day_count, pipe.opportunity_expected_go_live_date_date) as fcst_date,
'OPTI__' || pipe.opportunity as sales_merchant_id,
pipe.opportunity_owner as owner,
pipe.opportunity_name as merchant_name, 
pipe.opportunity_merchant_country as merchant_country,
pipe.vertical as vertical,
pipe.opportunity_status as opportunity_status,
pipe.opportunity_expected_go_live_date_date as sales_activation_date,
(pipe.mes_opportunity_amount)*first_year_sold_pct as pipeline_npv
from ( 
SELECT 
    salesforcemerchants.opportunity AS "opportunity",
    salesforcemerchants.opportunity_name AS "opportunity_name",
    DATE(salesforcemerchants.opportunity_close_date) AS "opportunity_close_date_date",
    salesforcemerchants.opportunity_owner AS "opportunity_owner",
    DATE(salesforcemerchants.opportunity_expected_go_live_date) AS "opportunity_expected_go_live_date_date",
    -- deal signed not live OR pipeline
    case when salesforcemerchants.opportunity_stage in ('Negotiating', 'Discovering Needs', 'Validating Fit', 'Proposing Solution')  THEN 'pipeline'
         when salesforcemerchants.opportunity_stage in ('Onboarding', 'Live')  THEN 'signed_not_live'
         else 'lost' end as opportunity_status, 
    opportunity_industry AS "vertical",
    salesforcemerchants.opportunity_merchant_country AS "opportunity_merchant_country",
    case when DATE(merchants.sales_funnel__activation_date) < '2017-01-28' THEN 1 ELSE 0 END AS "live_or_not",
    DATE(merchants.sales_funnel__activation_date) AS "sales_funnel__activation_date_date",
    --salesforcemerchants.opportunity_stage AS "opportunity_stage",
    (COALESCE(COALESCE( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(salesforcemerchants.opportunity_amount,0)*(1000000*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CONVERT(VARCHAR,salesforcemerchants.opportunity)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CONVERT(VARCHAR,salesforcemerchants.opportunity)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CONVERT(VARCHAR,salesforcemerchants.opportunity)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CONVERT(VARCHAR,salesforcemerchants.opportunity)),15),16) AS DECIMAL(38,0))) )  / (1000000*1.0), 0), 0))*(avg(opportunity_probability)/100) AS "mes_opportunity_amount",
    COALESCE(SUM(datediff (days, opportunity_close_date, getdate())), 0) AS "days_since_close_date"
FROM sales.salesforce AS salesforcemerchants
LEFT JOIN dim.merchants as merchants on stripe_merchant_id = merchants._id
WHERE 
/********
NEED TO UPDATE DATES BELOW    
********/
    
    
(merchants.sales_funnel__activation_date IS NULL OR merchants.sales_funnel__activation_date >= TIMESTAMP '2017-02-12')
AND salesforcemerchants.opportunity_expected_go_live_date >= TIMESTAMP '2017-02-06' -- include things that may be going live this week
AND salesforcemerchants.opportunity_expected_go_live_date <= TIMESTAMP '2017-12-31' -- include all opportunities expected to live this year
AND salesforcemerchants.opportunity_stage in ('Negotiating', 'Discovering Needs', 'Validating Fit', 'Proposing Solution', 'Onboarding', 'Live')

GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 8
 )  as pipe 
cross join usertables.day_backlog_curve as curve
where 
(curve.day_count between 0 and 364))


select 
  opportunity_status as data_type,
  to_char(date_trunc('year', fcst_date),'YYYY') as year,
  to_char(date_trunc('quarter', fcst_date), 'YYYY-MM') as quarter,
  to_char(date_trunc('week', fcst_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') as finance_week,
  case when date_trunc('quarter', fcst_date) = date_trunc('quarter', CURRENT_DATE) then 1 else 0 end as qtd, 
  case when date_trunc('week', fcst_date + '1 day'::interval)::date - '1 day'::interval = date_trunc('week', dateadd('day',-3, CURRENT_DATE) + '1 day'::interval)::date - '1 day'::interval then 1 else 0 end as this_week, 
  cc.sales_region as region,
  cc.sfdc_country_name as country,
  '' as sales_channel,
  case
  -- 1. filter team type first
  --when sales_location = 'Hub' then 'Hub' 
  when role = 'NBA' then 'NBA'
  -- UK verticals
  when cc.sales_region = 'UK' and vertical in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when cc.sales_region = 'UK' and vertical in ('Financial') then 'Financial Services'
  when cc.sales_region = 'UK' and vertical in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)', 'Real Estate') then 'Services, Software & Content'
  -- US/CA
  when cc.sfdc_country_name = 'United States' and vertical in ('B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Software & Content'
  when cc.sfdc_country_name = 'United States' and  vertical in ('Ticketing & Events', 'Financial', 'Healthcare', 'Professional Services', 'Other Services', 'Travel & Hosp', 'Real Estate') then 'Services'
  when cc.sfdc_country_name = 'United States' and  vertical in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when cc.sfdc_country_name = 'United States' and  vertical in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when cc.sfdc_country_name = 'United States' and  vertical is null then 'No industry'
  when cc.sfdc_country_name = 'Canada' then 'CA'  
  -- SouthernEU
  when cc.sales_region = 'Southern EU' then cc.sfdc_country_name
  -- NorthernEU
  when cc.sales_region = 'Northern EU' and cc.sfdc_country_name in ('DE','AT','CH') then 'DACH'
  when cc.sales_region = 'Northern EU' and cc.sfdc_country_name in ('BE','NL','LU') then 'BENELUX'
  when cc.sales_region = 'Northern EU' and cc.sfdc_country_name in ('NO', 'FI', 'SE', 'DK', 'IS') then 'BENELUX'  
  -- AU/NZ
  when cc.sales_region = 'AU' then cc.sfdc_country_name
  -- SG
  when cc.sales_region = 'SG' then cc.sfdc_country_name
  when cc.sales_region = 'New Markets' then cc.sfdc_country_name
  -- IE
  when cc.sales_region = 'IE' then cc.sfdc_country_name
  else 'other'
end AS sub_region,  
case
  -- UK verticals
  when cc.sales_region = 'UK' and vertical in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when cc.sales_region = 'UK' and vertical in ('Financial') then 'Financial Services'
  when cc.sales_region = 'UK' and vertical in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)', 'Real Estate') then 'Services, Software & Content'
  -- Standard verticals
  when vertical in ('B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Software & Content'
  when vertical in ('Ticketing & Events', 'Financial', 'Healthcare', 'Professional Services', 'Other Services', 'Travel & Hosp', 'Real Estate')
  then 'Services'
  when vertical in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when vertical in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when vertical is null then 'No industry'
  else 'other'
end
 AS vertical,  
  owner as owner,
  usr.role as sales_role,
  usr.team AS sales_location,
  sales_merchant_id as sales_merchant_id,
  merchant_name,
  'opportunity' as sales_category,
  sales_activation_date,
  case when datediff('d', sales_activation_date, fcst_date) >= 0 and datediff('d', sales_activation_date, fcst_date) < 91 then 1 else 0 end as ninety_day_live,
  case when datediff('d', sales_activation_date, fcst_date) >= 0 and datediff('d', sales_activation_date, fcst_date) < 366 then 1 else 0 end as first_year_sold,
  COALESCE(SUM(pipeline_npv), 0) AS npv_fixed_fx  
FROM daily_pipeline dp
JOIN country_code as cc ON dp.merchant_country = cc.sfdc_country_name
JOIN team_role as usr ON usr.sales_owner = dp.owner
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20


