/***************************************************
Step 1. Get the weekly processing volume
Generate processing volume query 

****************************************************/
-- 1. delete last week backup
drop table usertables.last_week_weekly_processing;
-- 2. rename this week to last week
alter table usertables.current_weekly_processing rename to last_week_weekly_processing;
-- 3. create base processing table
create table usertables.current_weekly_processing as (
select
'weekly_processing' as data_type,
to_char(date_trunc('year', ap.capture_date),'YYYY') as year,
to_char(date_trunc('quarter', ap.capture_date), 'YYYY-MM') as quarter,
to_char(date_trunc('week', ap.capture_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') as finance_week,
case when date_trunc('quarter', ap.capture_date) = date_trunc('quarter', CURRENT_DATE) then 1 else 0 end as qtd,
cc.sales_region as region,
cc.sfdc_country_name as country,
'' as sales_channel,
case
 -- 1. filter team type first
  --when sales_location = 'Hub' then 'Hub' 
  when sales_role = 'NBA' then 'NBA'
  -- UK verticals
  when cc.sales_region = 'UK' and m.sales__industry in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when cc.sales_region = 'UK' and m.sales__industry in ('Financial') then 'Financial Services'
  when cc.sales_region = 'UK' and m.sales__industry in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)', 'Real Estate') then 'Services, Software & Content'
  -- US/CA
  when cc.sfdc_country_name = 'US' and m.sales__industry in ('B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Software & Content'
  when cc.sfdc_country_name = 'US' and  m.sales__industry in ('Ticketing & Events', 'Financial', 'Healthcare', 'Professional Services', 'Other Services', 'Travel & Hosp', 'Real Estate')
  then 'Services'
  when cc.sfdc_country_name = 'US' and  m.sales__industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when cc.sfdc_country_name = 'US' and  m.sales__industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when cc.sfdc_country_name = 'US' and  m.sales__industry is null then 'No industry'
  when cc.sfdc_country_name = 'CA' then 'CA'  
  -- SouthernEU
  when cc.sales_region = 'SouthernEU' then cc.sfdc_country_name
  -- NorthernEU
  when cc.sales_region = 'NorthernEU' and cc.sfdc_country_name in ('DE','AT','CH') then 'DACH'
  when cc.sales_region = 'NorthernEU' and cc.sfdc_country_name in ('BE','NL','LU') then 'BENELUX'
  when cc.sales_region = 'NorthernEU' and cc.sfdc_country_name in ('NO', 'FI', 'SE', 'DK', 'IS') then 'BENELUX'  
  -- AU/NZ
  when cc.sales_region = 'AU' then cc.sfdc_country_name
  -- SG
  when cc.sales_region = 'SG' then cc.sfdc_country_name
  when cc.sales_region = 'New Markets' then cc.sfdc_country_name
  -- IE
  when cc.sales_region = 'IE' then cc.sfdc_country_name
  else 'other'
end
 AS sub_region,  

case
  -- UK verticals
  when cc.sales_region = 'UK' and m.sales__industry in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when cc.sales_region = 'UK' and m.sales__industry in ('Financial') then 'Financial Services'
  when cc.sales_region = 'UK' and m.sales__industry in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)', 'Real Estate') then 'Services, Software & Content'
  -- Standard verticals
  when m.sales__industry in ('B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Software & Content'
  when m.sales__industry in ('Ticketing & Events', 'Financial', 'Healthcare', 'Professional Services', 'Other Services', 'Travel & Hosp', 'Real Estate')
  then 'Services'
  when m.sales__industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when m.sales__industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when m.sales__industry is null then 'No industry'
  else 'other'
end
 AS vertical,  
m.sales__owner as owner,
usr.role as sales_role,
usr.team AS sales_location,
sales_merchant_id as sales_merchant_id,
m.sales__name AS merchant_name,
sales_funnel__activation_date as activation_date,
case when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 91 then 1 else 0 end as ninety_day_live,
case when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 then 1 else 0 end as first_year_sold,
COALESCE(SUM(ap.npv_usd_fixed / 100), 0) AS npv_fixed_fx

from aggregates.payments ap
JOIN dim.merchants AS m ON ap.sales_merchant_id = m.sales_merchant_id
JOIN usertables.mc_country_codes_csv as cc ON m.sales__merchant_country = cc.country_code
JOIN usertables.mc_team_role_csv as usr ON usr.sales_owner = m.sales__owner

where
capture_date > '2016-09-30'
and 
m.sales__is_sold = true
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18 );
