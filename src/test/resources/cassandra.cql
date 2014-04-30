DROP KEYSPACE emp_table;
CREATE KEYSPACE emp_table
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE emp_table;

CREATE TABLE emp(
  id int,
  name text,
  PRIMARY KEY(id, name)
);

INSERT INTO emp(id, name) VALUES (1000, 'Charles Black');
INSERT INTO emp(id, name) VALUES (1001, 'William Stark');
INSERT INTO emp(id, name) VALUES (1002, 'James Silver');
INSERT INTO emp(id, name) VALUES (1003, 'Tom Jacobs');
