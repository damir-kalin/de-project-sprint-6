drop table if exists STV2024031233__DWH.h_dialogs;

create table STV2024031233__DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id int,
    message_ts datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);