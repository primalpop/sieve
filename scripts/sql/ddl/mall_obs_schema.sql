CREATE TABLE IF NOT EXISTS MALL_OBSERVATION  (
  id varchar(50) NOT NULL,
  shop_name varchar(20) NOT NULL,
  obs_date date NOT NULL,
  obs_time time NOT NULL,
  user_interest varchar(20) DEFAULT NULL,
  device_id int NOT NULL,
  PRIMARY KEY (id)
) ;