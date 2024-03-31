drop table if exists STV2024031233__STAGING.group_log;

create table STV2024031233__STAGING.group_log
(
group_id int not null,
user_id int not null,
user_id_from int,
event varchar(10),
event_dt datetime
)
order by group_id, user_id, event_dt
segmented by hash(group_id, user_id, event_dt) all nodes
partition by event_dt::date
group by calendar_hierarchy_day(event_dt::date, 3, 2);