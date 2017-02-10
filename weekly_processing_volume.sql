/***************************************************

Generate processing volume query 

****************************************************/


alter table usertables.current_weekly_processing rename usertables.last_week_weekly_processing

create table usertables.current_weekly_processing as (
select
'weekly_processing' as data_type,
to_char(date_trunc('year', ap.capture_date),'YYYY') as year,
date_trunc('quarter', ap.capture_date) as quarter,
to_char(date_trunc('week', ap.capture_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') as finance_week,
case when date_trunc('quarter', ap.capture_date) = date_trunc('quarter', CURRENT_DATE) then 1 else 0 end as qtd,
cc.sales_region as region,
cc.sfdc_country_name as country,
'' as sales_channel,
case
  -- UK verticals
  when cc.sales_region = 'UK' and m.sales__industry in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when cc.sales_region = 'UK' and m.sales__industry in ('Financial') then 'Financial Services'
  when cc.sales_region = 'UK' and m.sales__industry in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Services, Software & Content'
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
sales__parent_merchant as parent_id,
m.sales__name AS merchant_name,
sales_funnel__activation_date as activation_date,
case when datediff('d', sales_funnel__activation_date, capture_date) < 91 then 1 else 0 end as ninety_day_live,
case when datediff('d', sales_funnel__activation_date, capture_date) < 366 then 1 else 0 end as first_year_sold,
COALESCE(SUM(ap.npv_usd_fixed / 100), 0) AS npv_fixed_fx

from aggregates.payments ap
INNER JOIN dim.merchants_backup AS m ON ap.sales_merchant_id = m._id
INNER JOIN usertables.mc_country_codes_csv as cc ON m.sales__merchant_country = cc.country_code
INNER JOIN usertables.mc_team_role_csv as usr ON usr.sales_owner = m.sales__owner

where
capture_date > '2016-09-30'
and 
m.sales__is_sold = true
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16 );
