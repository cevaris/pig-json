drop keyspace test_keyspace;
create keyspace test_keyspace with placement_strategy = 'SimpleStrategy' and strategy_options = {replication_factor : 2} and durable_writes = true;
use test_keyspace;
create column family emp;
-- update column family emp with
--   column_metadata = [
--     {column_name: id,   validation_class: IntegerType},
--     {column_name: name, validation_class: UTF8Type},
--   ];

assume emp keys as ascii;
assume emp comparator as ascii;
assume emp validator as ascii;
assume emp sub_comparator as ascii;

set emp['1']['name'] = 'Charles Black';
set emp['1']['name'] = 'William Stark';
set emp['1']['name'] = 'James Silver';
set emp['1']['name'] = 'Tom Jacobs';


-- CREATE KEYSPACE test_keyspace
--   WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

-- USE test_keyspace;

-- DROP TABLE emp;
-- CREATE TABLE emp(
--   id int,
--   name text,
--   PRIMARY KEY(id)
-- );

-- INSERT INTO emp(id, name) VALUES (1000, 'Charles Black');
-- INSERT INTO emp(id, name) VALUES (1001, 'William Stark');
-- INSERT INTO emp(id, name) VALUES (1002, 'James Silver');
-- INSERT INTO emp(id, name) VALUES (1003, 'Tom Jacobs');

