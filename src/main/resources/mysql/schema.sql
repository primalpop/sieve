/* Table creation statements for MySQL */

CREATE TABLE IF NOT EXISTS INFRASTRUCTURE  (
  NAME varchar(255) NOT NULL,
  INFRASTRUCTURE_TYPE varchar(255) DEFAULT NULL,
  FLOOR integer NOT NULL,
  REGION_NAME varchar(255) DEFAULT NULL,
  PRIMARY KEY (NAME)
) ;

CREATE TABLE IF NOT EXISTS USER  (
  EMAIL varchar(255) DEFAULT NULL UNIQUE,
  NAME varchar(255) DEFAULT NULL,
  ID varchar(255) NOT NULL,
  OFFICE varchar(255) DEFAULT NULL,
  PRIMARY KEY (ID)
 ) ;

CREATE TABLE IF NOT EXISTS USER_GROUP  (
  ID varchar(255) NOT NULL,
  DESCRIPTION varchar(255) DEFAULT NULL,
  NAME varchar(255) DEFAULT NULL,
  OWNER varchar(255) DEFAULT NULL,
  PRIMARY KEY (ID),
    FOREIGN KEY (OWNER) REFERENCES USER (ID)
) ;

CREATE TABLE IF NOT EXISTS USER_GROUP_MEMBERSHIP  (
  USER_ID varchar(255) NOT NULL,
  USER_GROUP_ID varchar(255) NOT NULL,
  PRIMARY KEY (USER_GROUP_ID, USER_ID),
   FOREIGN KEY (USER_ID) REFERENCES USER (ID),
   FOREIGN KEY (USER_GROUP_ID) REFERENCES USER_GROUP (ID)
) ;

CREATE TABLE IF NOT EXISTS SEMANTIC_OBSERVATION  (
  id varchar(255) NOT NULL,
  user_id varchar(255) NOT NULL,
  location_id varchar(255) NOT NULL,
  temperature varchar(255) DEFAULT NULL,
  energy varchar(255) DEFAULT NULL,
  activity varchar(255) DEFAULT NULL,
  timeStamp timestamp NOT NULL,
  PRIMARY KEY (id)
--   FOREIGN KEY (location_id) REFERENCES INFRASTRUCTURE (NAME),
--   FOREIGN KEY (user_id) REFERENCES USER (ID)
) ;


--CREATE TABLE IF NOT EXISTS USER_POLICY  (
--  id varchar(255) NOT NULL,
--  querier varchar(255) NOT NULL,
--  purpose varchar(255) NOT NULL,
--  enforcement_action varchar(255) DEFAULT NULL,
--  inserted_at timestamp NOT NULL,
--  PRIMARY KEY (id),
--  FOREIGN KEY (querier) REFERENCES USER (ID)
--) ;
--
--CREATE TABLE IF NOT EXISTS GROUP_POLICY  (
--  id varchar(255) NOT NULL,
--  querier varchar(255) NOT NULL,
--  purpose varchar(255) NOT NULL,
--  enforcement_action varchar(255) DEFAULT NULL,
--  inserted_at timestamp NOT NULL,
--  PRIMARY KEY (id),
--  FOREIGN KEY (querier) REFERENCES USER_GROUP (ID)
--) ;
--
--CREATE TABLE IF NOT EXISTS USER_POLICY_OBJECT_CONDITION  (
--  id integer NOT NULL AUTO_INCREMENT,
--  policy_id varchar(255) NOT NULL,
--  attribute varchar(255) NOT NULL,
--  attribute_type varchar(255) NOT NULL,
--  operator varchar(255) NOT NULL,
--  comp_value varchar(255) DEFAULT NULL,
--  PRIMARY KEY (id),
--  FOREIGN KEY (policy_id) REFERENCES USER_POLICY (id)
--) ;
--
--
--CREATE TABLE IF NOT EXISTS GROUP_POLICY_OBJECT_CONDITION  (
--  id integer NOT NULL AUTO_INCREMENT,
--  policy_id varchar(255) NOT NULL,
--  attribute varchar(255) NOT NULL,
--  attribute_type varchar(255) NOT NULL,sx
--  operator varchar(255) NOT NULL,
--  comp_value varchar(255) DEFAULT NULL,
--  PRIMARY KEY (id),
--  FOREIGN KEY (policy_id) REFERENCES GROUP_POLICY (id)
--) ;
--
--CREATE TABLE IF NOT EXISTS USER_GUARD  (
--  id varchar(255) NOT NULL,
--  querier varchar(255) NOT NULL,
--  purpose varchar(255) NOT NULL,
--  enforcement_action varchar(255) DEFAULT NULL,
--  last_updated timestamp NOT NULL,
--  dirty varchar(255) NOT NULL,
--  PRIMARY KEY (id),
--  FOREIGN KEY (querier) REFERENCES USER (ID)
--) ;
--
--CREATE TABLE IF NOT EXISTS GROUP_GUARD  (
--  id varchar(255) NOT NULL,
--  querier varchar(255) NOT NULL,
--  purpose varchar(255) NOT NULL,
--  enforcement_action varchar(255) DEFAULT NULL,
--  last_updated timestamp NOT NULL,
--  dirty varchar(255) NOT NULL,
--  PRIMARY KEY (id),
--  FOREIGN KEY (querier) REFERENCES USER_GROUP (ID)
--) ;
--
--
--CREATE TABLE IF NOT EXISTS USER_GUARD_EXPRESSION  (
--  id integer NOT NULL AUTO_INCREMENT,
--  guard_exp_id varchar(255) NOT NULL,
--  guard varchar(255) NOT NULL,
--  remainder TEXT NOT NULL,
--  PRIMARY KEY (id),
--  FOREIGN KEY (guard_exp_id) REFERENCES USER_GUARD (id)
--) ;
--
--CREATE TABLE IF NOT EXISTS GROUP_GUARD_EXPRESSION  (
--  id integer NOT NULL AUTO_INCREMENT,
--  guard_exp_id varchar(255) NOT NULL,
--  guard varchar(255) NOT NULL,
--  remainder TEXT NOT NULL,
--  PRIMARY KEY (id),
--  FOREIGN KEY (guard_exp_id) REFERENCES GROUP_GUARD (id)
--) ;
--

CREATE TABLE IF NOT EXISTS SIMPLE_POLICY  (
  id varchar(255) NOT NULL,
  querier varchar(255) NOT NULL,
  owner varchar(255) NOT NULL,
  purpose varchar(255) NOT NULL,
  uPol varchar(255) NOT NULL,
  lPol varchar(255) DEFAULT NULL,
  templPol varchar(255) DEFAULT NULL,
  tempgPol varchar(255) DEFAULT NULL,
  elPol varchar(255) DEFAULT NULL,
  egPol varchar(255) DEFAULT NULL,
  aPol varchar(255) DEFAULT NULL,
  tslPol timestamp DEFAULT NULL,
  tsgPol timestamp DEFAULT NULL,
  enforcement_action varchar(255) DEFAULT NULL,
  inserted_at timestamp NOT NULL,
  PRIMARY KEY (id)
) ;


--create index so_user_hash on SEMANTIC_OBSERVATION (user_id) using hash;
-- create index so_l_hash on SEMANTIC_OBSERVATION (location_id) using hash;
-- create index so_activity_hash on SEMANTIC_OBSERVATION (activity) using hash;

--
--CREATE INDEX so_ts ON SEMANTIC_OBSERVATION(timeStamp);
--CREATE INDEX so_u ON SEMANTIC_OBSERVATION(user_id);
--CREATE INDEX so_l ON SEMANTIC_OBSERVATION(location_id);
--CREATE INDEX so_e ON SEMANTIC_OBSERVATION (energy);
--CREATE INDEX so_t ON SEMANTIC_OBSERVATION (temperature);
--CREATE INDEX so_a ON SEMANTIC_OBSERVATION (activity);
--CREATE INDEX so_ul ON SEMANTIC_OBSERVATION (user_id,location_id);
--CREATE INDEX so_utS ON SEMANTIC_OBSERVATION (user_id,timeStamp);
--CREATE INDEX so_ue ON SEMANTIC_OBSERVATION (user_id,energy);
--CREATE INDEX so_ut ON SEMANTIC_OBSERVATION (user_id,temperature);
--CREATE INDEX so_ua ON SEMANTIC_OBSERVATION (user_id,activity);
--CREATE INDEX so_ltS ON SEMANTIC_OBSERVATION (location_id,timeStamp);
--CREATE INDEX so_le ON SEMANTIC_OBSERVATION (location_id,energy);
--CREATE INDEX so_lt ON SEMANTIC_OBSERVATION (location_id,temperature);
--CREATE INDEX so_la ON SEMANTIC_OBSERVATION (location_id,activity);
--CREATE INDEX so_tSe ON SEMANTIC_OBSERVATION (timeStamp,energy);
--CREATE INDEX so_tSt ON SEMANTIC_OBSERVATION (timeStamp,temperature);
--CREATE INDEX so_tSa ON SEMANTIC_OBSERVATION (timeStamp,activity);
--CREATE INDEX so_et ON SEMANTIC_OBSERVATION (energy,temperature);
--CREATE INDEX so_ea ON SEMANTIC_OBSERVATION (energy,activity);
--CREATE INDEX so_ta ON SEMANTIC_OBSERVATION (temperature,activity);
--CREATE INDEX so_ule ON SEMANTIC_OBSERVATION (user_id,location_id,energy);
--CREATE INDEX so_ult ON SEMANTIC_OBSERVATION(user_id, location_id, timeStamp);
--CREATE INDEX so_ultemp ON SEMANTIC_OBSERVATION (user_id,location_id,temperature);
--CREATE INDEX so_ula ON SEMANTIC_OBSERVATION (user_id,location_id,activity);
--CREATE INDEX so_utSe ON SEMANTIC_OBSERVATION (user_id,timeStamp,energy);
--CREATE INDEX so_utSt ON SEMANTIC_OBSERVATION (user_id,timeStamp,temperature);
--CREATE INDEX so_utSa ON SEMANTIC_OBSERVATION (user_id,timeStamp,activity);
--CREATE INDEX so_uet ON SEMANTIC_OBSERVATION (user_id,energy,temperature);
--CREATE INDEX so_uea ON SEMANTIC_OBSERVATION (user_id,energy,activity);
--CREATE INDEX so_uta ON SEMANTIC_OBSERVATION (user_id,temperature,activity);
--CREATE INDEX so_ltSe ON SEMANTIC_OBSERVATION (location_id,timeStamp,energy);
--CREATE INDEX so_ltSt ON SEMANTIC_OBSERVATION (location_id,timeStamp,temperature);
--CREATE INDEX so_ltSa ON SEMANTIC_OBSERVATION (location_id,timeStamp,activity);
--CREATE INDEX so_let ON SEMANTIC_OBSERVATION (location_id,energy,temperature);
--CREATE INDEX so_lea ON SEMANTIC_OBSERVATION (location_id,energy,activity);
--CREATE INDEX so_lta ON SEMANTIC_OBSERVATION (location_id,temperature,activity);
--CREATE INDEX so_tSet ON SEMANTIC_OBSERVATION (timeStamp,energy,temperature);
--CREATE INDEX so_tSea ON SEMANTIC_OBSERVATION (timeStamp,energy,activity);
--CREATE INDEX so_tSta ON SEMANTIC_OBSERVATION (timeStamp,temperature,activity);
--CREATE INDEX so_eta ON SEMANTIC_OBSERVATION (energy,temperature,activity);
--CREATE INDEX so_ultSe ON SEMANTIC_OBSERVATION (user_id,location_id,timeStamp,energy);
--CREATE INDEX so_ultSt ON SEMANTIC_OBSERVATION (user_id,location_id,timeStamp,temperature);
--CREATE INDEX so_ultSa ON SEMANTIC_OBSERVATION (user_id,location_id,timeStamp,activity);
--CREATE INDEX so_ulet ON SEMANTIC_OBSERVATION (user_id,location_id,energy,temperature);
--CREATE INDEX so_ulea ON SEMANTIC_OBSERVATION (user_id,location_id,energy,activity);
--CREATE INDEX so_ulta ON SEMANTIC_OBSERVATION (user_id,location_id,temperature,activity);
--CREATE INDEX so_utSet ON SEMANTIC_OBSERVATION (user_id,timeStamp,energy,temperature);
--CREATE INDEX so_utSea ON SEMANTIC_OBSERVATION (user_id,timeStamp,energy,activity);
--CREATE INDEX so_utSta ON SEMANTIC_OBSERVATION (user_id,timeStamp,temperature,activity);
--CREATE INDEX so_ueta ON SEMANTIC_OBSERVATION (user_id,energy,temperature,activity);
--CREATE INDEX so_ltSet ON SEMANTIC_OBSERVATION (location_id,timeStamp,energy,temperature);
--CREATE INDEX so_ltSea ON SEMANTIC_OBSERVATION (location_id,timeStamp,energy,activity);
--CREATE INDEX so_ltSta ON SEMANTIC_OBSERVATION (location_id,timeStamp,temperature,activity);
--CREATE INDEX so_leta ON SEMANTIC_OBSERVATION (location_id,energy,temperature,activity);
--CREATE INDEX so_tSeta ON SEMANTIC_OBSERVATION (timeStamp,energy,temperature,activity);
--CREATE INDEX so_ultSet ON SEMANTIC_OBSERVATION (user_id,location_id,timeStamp,energy,temperature);
--CREATE INDEX so_ultSea ON SEMANTIC_OBSERVATION (user_id,location_id,timeStamp,energy,activity);
--CREATE INDEX so_ultSta ON SEMANTIC_OBSERVATION (user_id,location_id,timeStamp,temperature,activity);
--CREATE INDEX so_uleta ON SEMANTIC_OBSERVATION (user_id,location_id,energy,temperature,activity);
--CREATE INDEX so_utSeta ON SEMANTIC_OBSERVATION (user_id,timeStamp,energy,temperature,activity);
--CREATE INDEX so_ltSeta ON SEMANTIC_OBSERVATION (location_id,timeStamp,energy,temperature,activity);
--CREATE INDEX so_ultSeta ON SEMANTIC_OBSERVATION (user_id,location_id,timeStamp,energy,temperature,activity);




