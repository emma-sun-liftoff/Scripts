# Do not change anything, this is good query :]

SELECT DISTINCT ad_format 
, logical_size 
, creative_type 
from analytics.daily 
where date_diff('day', from_iso8601_timestamp(dt), current_date) <= 2
AND logical_size <> 'UNKNOWN_LOGICAL_SIZE'
AND logical_size <> 'UNMATCHED'
AND creative_type <> 'N/A'
order by 1






