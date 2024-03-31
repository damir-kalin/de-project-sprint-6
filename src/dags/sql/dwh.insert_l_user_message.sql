INSERT INTO STV2024031233__DWH.l_user_message(hk_l_user_message,hk_user_id,hk_message_id,load_dt,load_src)
select
hash(hu.hk_user_id, hd.hk_message_id),
hu.hk_user_id,
hd.hk_message_id,
now() as load_dt,
's3' as load_src
from STV2024031233__STAGING.dialogs d 
left join STV2024031233__DWH.h_users hu on d.message_from = hu.user_id
left join STV2024031233__DWH.h_dialogs hd on d.message_id = hd.message_id
where hash(hu.hk_user_id, hd.hk_message_id) not in (select hk_l_user_message from STV2024031233__DWH.l_user_message);