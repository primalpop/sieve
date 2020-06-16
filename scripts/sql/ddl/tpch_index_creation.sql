create index cust_hash on ORDERS(O_CUSTKEY) using hash;
create index total_price_tree on ORDERS(O_TOTALPRICE) using btree;
create index date_tree on ORDERS(O_ORDERDATE) using btree;
create index priority_hash on ORDERS(O_ORDERPRIORITY) using hash;
create index profile_hash on ORDERS(O_PROFILE) using hash;
create index clerk_hash on ORDERS(O_CLERK) using hash;


