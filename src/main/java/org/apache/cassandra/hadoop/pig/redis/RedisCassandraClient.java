package org.apache.cassandra.hadoop.pig.redis;

import java.util.Arrays;

public class RedisCassandraClient {
	
	private CassandraClient cassandra;
	private RedisClient redis;
	
	public RedisCassandraClient(String cassandraUrl, String keyspace, String columnFamily, String redisUrl, String referenceSet) {
		this.cassandra = new CassandraClient(cassandraUrl, keyspace, columnFamily);
		this.redis = new RedisClient(referenceSet);
	}
	
	public RedisClient getRedis() {
		return redis;
	}
	
	public CassandraClient getCassandra() {
		return cassandra;
	}
}
