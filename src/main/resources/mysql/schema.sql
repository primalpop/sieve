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


--CREATE INDEX IF NOT EXISTS semantic_observation_timestamp_idx ON SEMANTIC_OBSERVATION(timeStamp);
--CREATE INDEX semantic_observation_user_idx ON SEMANTIC_OBSERVATION(user_id);
--CREATE INDEX semantic_observation_location_idx ON SEMANTIC_OBSERVATION(location_id);