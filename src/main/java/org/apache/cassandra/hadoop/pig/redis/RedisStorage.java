package org.apache.cassandra.hadoop.pig.redis;

import java.io.IOException;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.tools.ant.types.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisStorage extends LoadFunc {
	
	private static final Logger logger = LoggerFactory.getLogger(RedisStorage.class);
	private RecordReader reader;

	@Override
	public void setLocation(String location, Job job) throws IOException {
		logger.debug("Location: " +location + " Job: " + job.getJobName());
		System.out.println("Location: " +location + " Job: " + job.getJobName());
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		return null;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		this.reader = reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
