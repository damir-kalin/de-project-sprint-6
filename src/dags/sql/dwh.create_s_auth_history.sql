drop table if exists STV2024031233__DWH.s_auth_history;

create table STV2024031233__DWH.s_auth_history
(
hk_l_user_group_activity bigint primary key,
user_id_from bigint,
event varchar(10),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_l_user_group_activity all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);