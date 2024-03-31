drop table if exists STV20224031233__DWH.l_groups_dialogs;

create table STV2024031233__DWH.l_groups_dialogs
(
hk_l_groups_dialogs bigint primary key,
hk_message_id bigint not null constraint fk_l_groups_dialogs_message references STV2024031233__DWH.h_dialogs (hk_message_id),
hk_group_id bigint not null constraint fk_l_groups_dialogs_group references STV2024031233__DWH.h_groups	(hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_l_groups_dialogs all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);