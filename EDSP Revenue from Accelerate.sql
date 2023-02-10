select 	event_time
		, account_id
		, a.name 
		, SUM(adv_spend) as adv_spend
		, SUM(pub_revenue) as pub_revenue
from edsp_report e 
	LEFT JOIN accounts a 
		ON e.account_id = a.id 
where event_time >= DATE_ADD('day',-<Parameters.Days of Data (EDSP)>,GETDATE())
	and e.account_id = '5f6413c9612b1a0015099993'
group by 1,2,3