package org.apache.cassandra.hadoop.pig.redis;

import static org.junit.Assert.*;

import org.junit.Test;

public class CassandraClientTest {

	@Test
	public void testSelectAll() {
		CassandraClient client = new CassandraClient("test_keyspace","emp");
		client.selectAll();
	}
	
//	@Test
//	public void testGetSchema() {
//		CassandraClient client = new CassandraClient("test_keyspace","tweets");
//		client.getSchema();
//	}

}
