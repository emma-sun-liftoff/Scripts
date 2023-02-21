SELECT 
count(distinct id)
, CASE 
    WHEN daily_revenue_limit >= 0 AND daily_revenue_limit < 100 THEN '[0,100)'
    WHEN daily_revenue_limit >= 100 AND daily_revenue_limit < 200 THEN '[100,200)'
    WHEN daily_revenue_limit >= 200 AND daily_revenue_limit < 300 THEN '[200,300)'
    WHEN daily_revenue_limit >= 300 AND daily_revenue_limit < 400 THEN '[300,400)'
    WHEN daily_revenue_limit >= 400 AND daily_revenue_limit < 500 THEN '[400,500)'
    WHEN daily_revenue_limit >= 500 AND daily_revenue_limit < 600 THEN '[500,600)'
    WHEN daily_revenue_limit >= 600 AND daily_revenue_limit < 700 THEN '[600,700)'
    WHEN daily_revenue_limit >= 700 AND daily_revenue_limit < 800 THEN '[700,800)'
    WHEN daily_revenue_limit >= 800 AND daily_revenue_limit < 900 THEN '[800,900)'
    WHEN daily_revenue_limit >= 900 AND daily_revenue_limit < 1000 THEN '[900,1000)'
    WHEN daily_revenue_limit >= 1000 AND daily_revenue_limit < 1500 THEN '[1000,1500)'
    WHEN daily_revenue_limit >= 1500 AND daily_revenue_limit < 2000 THEN '[1500,2000)'
    WHEN daily_revenue_limit >= 2000 AND daily_revenue_limit < 2500 THEN '[2000,2500)'
    WHEN daily_revenue_limit >= 2500 AND daily_revenue_limit < 3000 THEN '[2500,3000)'
    WHEN daily_revenue_limit >= 3000 AND daily_revenue_limit < 3500 THEN '[3000,3500)'
    WHEN daily_revenue_limit >= 3500 AND daily_revenue_limit < 4000 THEN '[3500,4000)'
    WHEN daily_revenue_limit >= 4000 AND daily_revenue_limit < 4500 THEN '[4000,4500)'
    WHEN daily_revenue_limit >= 4500 AND daily_revenue_limit < 5000 THEN '[4500,5000)'
    WHEN daily_revenue_limit >= 5000 AND daily_revenue_limit < 5500 THEN '[5000,5500)'
    WHEN daily_revenue_limit >= 5500 AND daily_revenue_limit < 6000 THEN '[5500,6000)'
    WHEN daily_revenue_limit >= 6000 AND daily_revenue_limit < 6500 THEN '[6000,6500)' 
    WHEN daily_revenue_limit >= 6500 AND daily_revenue_limit < 7000 THEN '[6500,7000)'   
    WHEN daily_revenue_limit >= 7000 AND THEN '[7000,+)'
    ELSE 'N/A'
END AS cap_range
FROM  public.campaigns 
WHERE created_at > '2022-11-28'
 AND current_optimization_state IN ('cpr','cpr-vt','cprv3','cprv3-vt')
AND state = 'enabled'
group by 2


SELECT 
count(distinct id)
, CASE 
    WHEN daily_revenue_limit >= 0 AND daily_revenue_limit < 100000 THEN '[0,100000)'
    WHEN daily_revenue_limit >= 100000 AND daily_revenue_limit < 200000 THEN '[100000,200000)'  
    WHEN daily_revenue_limit >= 200000 AND THEN '[200000,+)'
    ELSE 'N/A'
END AS cap_range
FROM  public.campaigns 
WHERE created_at > '2022-11-28'
 AND current_optimization_state IN ('cpr','cpr-vt','cprv3','cprv3-vt')
AND state = 'enabled'
group by 2



SELECT
campaign_id 
, SUM(installs) AS installs 
, SUM(CAST(revenue_micros as DOUBLE)/1000000) as GR

FROM analytics.daily 
WHERE dt > '2022-09-21' AND dt < '2022-09-29'
GROUP BY 1
