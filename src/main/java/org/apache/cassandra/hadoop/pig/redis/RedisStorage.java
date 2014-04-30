package org.apache.cassandra.hadoop.pig.redis;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

public class RedisStorage extends LoadFunc {

	@Override
	public void setLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
