package org.apache.cassandra.hadoop.pig.redis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import redis.clients.jedis.Jedis;

public class RedisClient {
	
	private String host = "localhost";
	private int port = 6379;
	private String referenceSet;
	private Jedis client;
	
	private static Pattern pattern = Pattern.compile(
            ":(.*?)=>\"(.*?)\"",
            (Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL));

	
	public RedisClient(String referenceSet) {
		this.referenceSet = referenceSet;
		this.client = new Jedis(host,port);
	}
	
	public static String[] parseRubyHash(String hash){
		String[] result = new String[2];
		//apply the pattern to our string content
	    Matcher matcher = pattern.matcher(hash);
	    int index = 0;
	    //while there are matches found
	    while (matcher.find()) {
	    	result[index++] = matcher.group(2).toString();
	    }
		return result;
		
	}
	public Set<String> all(){
		return client.zrange(referenceSet, 0, -1);
	}
	

}
