drop keyspace test_keyspace;
create keyspace test_keyspace with placement_strategy = 'SimpleStrategy' 
  and strategy_options = {replication_factor : 2}
  and durable_writes = true;

use test_keyspace;


create column family emp with 
  key_validation_class=UTF8Type 
    and comparator=UTF8Type 
    and default_validation_class=UTF8Type;


-- update column family emp with
--   column_metadata = [
--     {column_name: id,   validation_class: UTF8Type},
--     {column_name: name, validation_class: UTF8Type},
--   ];

-- assume emp keys as utf8;
-- assume emp comparator as utf8;
-- assume emp validator as utf8;
-- assume emp sub_comparator as utf8;

set emp['1']['name'] = 'Charles Black';
set emp['2']['name'] = 'William Stark';
set emp['3']['name'] = 'James Silver';
set emp['4']['name'] = 'Tom Jacobs';


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

