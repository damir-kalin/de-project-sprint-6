drop table if exists STV2024031233__STAGING.dialogs;

create table STV2024031233__STAGING.dialogs(
	message_id int not null primary key,
	message_ts datetime,
	message_from int,
	message_to int,
	message varchar(1000),
	message_group int
)
order by message_id
segmented by hash(message_id) all nodes
partition by message_ts::date
group by calendar_hierarchy_day(message_ts::date, 3, 2);