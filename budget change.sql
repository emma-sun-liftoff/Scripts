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