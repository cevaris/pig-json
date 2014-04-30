package org.apache.cassandra.hadoop.pig.redis;

import static org.junit.Assert.*;

import org.junit.Test;

public class CassandraClientTest {

//	@Test
	public void testSelectAll() {
		CassandraClient client = new CassandraClient("test_keyspace","tweets");
		client.selectAll();
	}
	
//	@Test
	public void testSelectOne() {
		CassandraClient client = new CassandraClient("test_keyspace","tweets");
		client.selectOne("jeffcoflood:2013260:5", "379809860197814272");
	}
	
	@Test
	public void testSelectOneProduction() {
		String seeds = "epic-n0.int.colorado.edu:9160,epic-n1.int.colorado.edu:9160, epic-n2.int.colorado.edu:9160,epic-n3.int.colorado.edu:9160";
		CassandraClient client = new CassandraClient(seeds, "EPIC","Filter_Tweet");
		System.out.print(client.selectOne("jeffcoflood:2013260:5", "379809860197814272"));
	}
	

}
