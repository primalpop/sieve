-- in postgres
create index shop_hash on mall_observation using hash (shop_name);
create index interest_hash on mall_observation using hash (user_interest);
create index device_hash on mall_observation using hash (device_hash);
create index obs_time_tree on mall_observation using btree (obs_time);
create index obs_date_tree on mall_observation using btree (obs_date);