    select customer_id,
    case when max_customer_level = 1 then 'Launch'
         when max_customer_level = 2 then 'Bronze'
         when max_customer_level = 3 then 'Silver'
         when max_customer_level = 4 then 'Gold'
         when max_customer_level = 5 then 'Platinum' end as customer_level
    from (select distinct customer_id,
                          max(case when salesforce_account_level = 'Launch' then 1
                                   when salesforce_account_level = 'Low' then 2
                                   when salesforce_account_level = 'Bronze' then 2
                                   when salesforce_account_level = 'Medium' then 3
                                   when salesforce_account_level = 'Silver' then 3
                                   when salesforce_account_level = 'Gold' then 4
                                   when salesforce_account_level = 'High' then 5
                                   when salesforce_account_level = 'Platinum' then 5 end) as max_customer_level
          from pinpoint.public.apps
          group by 1) a