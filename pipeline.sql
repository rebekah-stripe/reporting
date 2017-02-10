
select  
dateadd(day, curve.day_count, pipe.opportunity_expected_go_live_date_date) as fcst_date,
'OPTI__' || pipe.opportunity as unified_merchant_id,
pipe.opportunity_owner as owner,
pipe.opportunity_name as merchant_name, 
pipe.opportunity_merchant_country,
pipe.vertical,
pipe.opportunity_status,
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
    case
  -- UK verticals
  when salesforcemerchants.opportunity_merchant_country = 'United Kingdom' and opportunity_industry in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when salesforcemerchants.opportunity_merchant_country = 'United Kingdom' and opportunity_industry in ('Financial') then 'Financial Services'
  when salesforcemerchants.opportunity_merchant_country = 'United Kingdom' and opportunity_industry in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Services, Software & Content'
  -- all other verticals


  when opportunity_industry in ('Ticketing & Events', 'Financial', 'Healthcare', 'Professional Services', 'Other Services', 'Travel & Hosp', 'Real Estate') then 'Services'
  when opportunity_industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when opportunity_industry in ('B2B', 'B2C Software', 'Content', 'Other Software & Content') then 'Software & Content'
  when opportunity_industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other') then 'Public Sector'
  when opportunity_industry is null then 'No industry'
  else 'Other'
end
 AS "vertical",
    salesforcemerchants.opportunity_merchant_country AS "opportunity_merchant_country",
    case when DATE(merchants.unified_funnel__activation_date) < '2017-01-28' THEN 1 ELSE 0 END AS "live_or_not",
    DATE(merchants.unified_funnel__activation_date) AS "unified_funnel__activation_date_date",
    --salesforcemerchants.opportunity_stage AS "opportunity_stage",
    (COALESCE(COALESCE( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(salesforcemerchants.opportunity_amount,0)*(1000000*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CONVERT(VARCHAR,salesforcemerchants.opportunity)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CONVERT(VARCHAR,salesforcemerchants.opportunity)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CONVERT(VARCHAR,salesforcemerchants.opportunity)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CONVERT(VARCHAR,salesforcemerchants.opportunity)),15),16) AS DECIMAL(38,0))) )  / (1000000*1.0), 0), 0))*(avg(opportunity_probability)/100) AS "mes_opportunity_amount",
    COALESCE(SUM(datediff (days, opportunity_close_date, getdate())), 0) AS "days_since_close_date"
FROM sales.salesforce AS salesforcemerchants
LEFT JOIN dim.merchants as merchants on stripe_merchant_id = merchants._id
WHERE 
--(salesforcemerchants.opportunity_stage = 'Onboarding' OR salesforcemerchants.opportunity_stage = 'Live') AND 
(merchants.unified_funnel__activation_date IS NULL OR merchants.unified_funnel__activation_date >= TIMESTAMP '2017-02-06')
AND salesforcemerchants.opportunity_expected_go_live_date >= TIMESTAMP '2017-02-06'
AND salesforcemerchants.opportunity_expected_go_live_date <= TIMESTAMP '2017-03-31'
AND salesforcemerchants.opportunity_stage in ('Negotiating', 'Discovering Needs', 'Validating Fit', 'Proposing Solution', 'Onboarding', 'Live')

GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 8
 )  as pipe 
cross join usertables.day_backlog_curve as curve
where 
(curve.day_count between 0 and 364)
