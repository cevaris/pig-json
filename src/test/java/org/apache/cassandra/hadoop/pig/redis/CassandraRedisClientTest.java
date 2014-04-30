package org.apache.cassandra.hadoop.pig.redis;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

public class CassandraRedisClientTest {

	@Test
	public void resolve() {
		String seeds = "epic-n0.int.colorado.edu:9160,epic-n1.int.colorado.edu:9160, epic-n2.int.colorado.edu:9160,epic-n3.int.colorado.edu:9160";
		RedisCassandraClient rcc = new RedisCassandraClient(seeds, "EPIC", "Filter_Tweet", "localhost:6379","2012 Casa Grande Explosion-ref" );

		for(String reference : rcc.getRedis().all()){
			String[] tupleRef = RedisClient.parseRubyHash(reference);
			
			String rowKey = tupleRef[0];
			String tweetId = tupleRef[1];
			String tweetJSON = rcc.getCassandra().selectOne(rowKey, tweetId);
			System.out.println(String.format("[%s][%s] = %s", rowKey, tweetId, tweetJSON));
			
		}
	}

}
		