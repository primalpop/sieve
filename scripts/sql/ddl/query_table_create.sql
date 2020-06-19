CREATE TABLE IF NOT EXISTS QUERIES  (
  id INT NOT NULL AUTO_INCREMENT,
  template INT NOT NULL,
  query_statement TEXT NOT NULL,
  selectivity float(13,8) NOT NULL,
  selectivity_type varchar(64) NOT NULL,
  inserted_at timestamp NOT NULL,
  PRIMARY KEY (ID)
 ) ;