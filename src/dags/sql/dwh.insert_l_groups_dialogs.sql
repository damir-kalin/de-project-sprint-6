INSERT INTO STV2024031233__DWH.l_groups_dialogs (hk_l_groups_dialogs,hk_message_id,hk_group_id,load_dt,load_src)
select
hash(hd.hk_message_id, hg.hk_group_id),
hd.hk_message_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from STV2024031233__STAGING.dialogs d 
left join STV2024031233__DWH.h_dialogs hd on d.message_id = hd.message_id 
inner join STV2024031233__DWH.h_groups hg on d.message_group = hg.group_id 
where hash(hd.hk_message_id, hg.hk_group_id) not in (select hk_l_groups_dialogs from STV2024031233__DWH.l_groups_dialogs);