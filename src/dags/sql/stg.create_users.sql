drop table if exists STV2024031233__STAGING.users;

create table STV2024031233__STAGING.users(
	id int not null primary key,
	chat_name varchar(200),
	registration_dt datetime,
	country varchar(200),
	age int
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES;