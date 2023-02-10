with funnel as (
    
    select
        dt
        , concat(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') as at
        , concat(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') as impression_at
        , null as install_at
        , bid__campaign_id as campaign_id
        , CASE 
            WHEN bid__bid_request__exchange = 'VUNGLE' THEN 'Vungle'
        ELSE 'Non-Vungle'
        END AS exchange_group
        , cast(json_extract(from_utf8(bid__bid_request__raw), '$.imp[0].ext.pptype') as integer) as pptype
        , spend_micros
        , revenue_micros
        , 0 as installs
    from rtb.impressions_with_bids a
    where dt = '{{ dt }}'
    
    union all
    
    select
        dt
        , concat(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') as at
        , concat(substr(to_iso8601(date_trunc('hour', from_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') as impression_at
        , concat(substr(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') as install_at
        , ad_click__impression__bid__campaign_id as campaign_id
        , CASE 
            WHEN ad_click__impression__bid__bid_request__exchange = 'VUNGLE' THEN 'Vungle'
        ELSE 'Non-Vungle'
        END AS exchange_group
        , cast(json_extract(from_utf8(ad_click__impression__bid__bid_request__raw), '$.imp[0].ext.pptype') as integer) as pptype
        , 0 as spend_micros
        , 0 as revenue_micros
        , 1 as installs
    from rtb.matched_installs a
    where dt = '{{ dt }}'
        and for_reporting = true
)

select
    at
    , impression_at
    , install_at
    , a.campaign_id as campaign_id
    , sum(spend_micros) as spend_micros
    , sum(revenue_micros) as revenue_micros
    , sum(CAST(revenue_micros as DOUBLE)/1000000) - sum(CAST(spend_micros as DOUBLE)/1000000) as Accelerate_NR
    , sum(CASE WHEN exchange_group = 'Vungle' THEN
            CASE 
                WHEN pptype = 1 THEN CAST(spend_micros as DOUBLE)/1000000 * 0.9 * 0.3526
                WHEN pptype = 2 THEN CAST(spend_micros as DOUBLE)/1000000 * 0.9 * 0.4437
                WHEN pptype = 3 THEN CAST(spend_micros as DOUBLE)/1000000 * 0.9 * 0.3016
                ELSE CAST(spend_micros as DOUBLE)/1000000 * 0.9 * 0.3319
            END
          END) as V_NR
    , sum(installs) as installs
from funnel a
left join pinpoint.public.campaigns f
    on f.id = a.campaign_id
group by 1,2,3,4

