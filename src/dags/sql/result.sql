with user_group_messages as (
    select 
    	lugd.hk_group_id,
    	count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from STV2024031233__DWH.l_groups_dialogs lugd
    	inner join STV2024031233__DWH.h_dialogs hd on lugd.hk_message_id = hd.hk_message_id
    	inner join STV2024031233__DWH.l_user_message lum on hd.hk_message_id = lum.hk_message_id
    group by lugd.hk_group_id
)
,user_group_log as (
   select 
	luga.hk_group_id,
	count(distinct luga.hk_user_id) as cnt_added_users
from STV2024031233__DWH.l_user_group_activity luga 
	left join (select distinct sah.hk_l_user_group_activity 
		from STV2024031233__DWH.s_auth_history sah 
		where sah.event = 'add') a on  luga.hk_l_user_group_activity = a.hk_l_user_group_activity
group by luga.hk_group_id  
)
select 
	ugm.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	round(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users, 2) as group_conversion
from user_group_messages ugm
	left join user_group_log ugl on ugm.hk_group_id = ugl.hk_group_id
order by round(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users, 2) desc; 