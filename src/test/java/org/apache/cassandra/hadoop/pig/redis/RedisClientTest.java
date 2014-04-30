package org.apache.cassandra.hadoop.pig.redis;

import static org.junit.Assert.*;

import org.junit.Test;

public class RedisClientTest {

	@Test
	public void getReferences() {
		RedisClient client = new RedisClient("2013 October NSW Bushfires-ref");
		client.all();
	}

}
