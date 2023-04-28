case 
    when coalesce(bid_request__device__platform_specific_id_sha1,'') <> '' then bid_request__device__platform_specific_id_sha1
    when coalesce(bid_request__device__idfv, '') <>'' then bid_request__device__idfv
    when coalesce(bid_request__pods__app_specific_id__id,'') <> '' then bid_request__pods__app_specific_id__id
    when coalesce(bid_request__device__model_data__name, bid_request__device__user_agent,'') <> '' 
      then coalesce(bid_request__device__model_data__name, bid_request__device__user_agent) || bid_request__device__geo__ip || bid_request__device__language
    else null 
  end as user_id
