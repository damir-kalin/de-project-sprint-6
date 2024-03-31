drop table if exists STV2024031233__STAGING.groups;

create table STV2024031233__STAGING.groups(
	id int not null primary key,
	admin_id int,
	group_name varchar(100),
	registration_dt datetime,
	is_private boolean
)
order by id, admin_id
segmented by hash(id) all nodes
partition by registration_dt::date
group by calendar_hierarchy_day(registration_dt::date, 3, 2);