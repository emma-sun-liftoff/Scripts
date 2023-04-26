# codes used in superset

select cast(ec.logged_at as DATE ) as date,
  ec.old_values -> 'daily_revenue_limit'  as old_drl,
  ec.new_values -> 'daily_revenue_limit'  as new_drl
FROM elephant_changes ec
  join campaigns c on ec.row_id = c.id
WHERE ec.operation = 'update'
  AND ec.table_name IN ('campaigns')
  AND ec.old_values -> 'daily_revenue_limit' is not null
  and c.id = 24664
 order by 1
 
 
 
 --- found paused campaigns
SELECT 
DISTINCT row_id AS campaign_id
, json_extract_scalar(new_values, '$.state') AS state
, json_extract_scalar(new_values, '$.state_last_changed_at') AS state_last_changed_at
from pinpoint.public.elephant_changes   
where row_id in (14132,
27318,
28267,
28457,
28461)
AND table_name = 'campaigns'
AND operation = 'update'
AND json_extract_scalar(new_values, '$.state') = 'paused'
ORDER BY 1,3 DESC 
