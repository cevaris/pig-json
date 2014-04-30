package org.apache.cassandra.hadoop.pig.redis;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URLClassLoader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.junit.Before;
import org.junit.Test;

public class CqlStorageTest {
	
	
	private ExecType execType = ExecType.LOCAL;
    private static PigServer pig;
    private PigContext pigContext = new PigContext(execType, new Properties());
	
	@Before
    public void setUp() throws IOException, URISyntaxException {
    	pig = new PigServer(execType);
    	setupDB();        
    }

	private void setupDB() {
		File file = new File("src/test/resources/cassandra.cql");
		Process p;
		StringBuilder output = new StringBuilder();
		try {
			String command = "/usr/local/cassandra/bin/cqlsh -f " +file.getAbsolutePath();
			System.out.println(command);
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
			reader.close();
			
			reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Created Database\n"+output.toString());
	}

	@Test
	public void testCassandraQuery() throws IOException {
		pigContext.connect();
        pig.registerQuery("employees = LOAD 'cql://test_keyspace/emp' USING org.apache.cassandra.hadoop.pig.redis.CqlStorage();");
//        pig.registerQuery("b = foreach a generate FLATTEN(json#'entities') as entities;");
//        pig.registerQuery("c = foreach b generate flatten(entities#'urls') as urls;");
//        pig.registerQuery("d = foreach c generate flatten(urls#'url') as url;");
        
//        List<Tuple> expectedResults = buildExpectedNestedJsonResults();
        Iterator<Tuple> iterator = pig.openIterator("employees");
        while (iterator.hasNext()) {
//        	Tuple expected = expectedResults.remove(0);
        	Tuple current = iterator.next();
//            assertEquals(expected.toString(), current.toString());
        	System.out.println(current);
        }
    }

    private List<Tuple> buildExpectedNestedJsonResults() {
        List<Tuple> expectedResults = new LinkedList<Tuple>();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        expectedResults.add(tupleFactory.newTuple("http://t.co/IOleSP7Csz"));
        expectedResults.add(tupleFactory.newTuple("http://t.co/IOleSP7Csz"));
        expectedResults.add(tupleFactory.newTuple("http://t.co/IOleSP7Csz"));
        expectedResults.add(tupleFactory.newTuple("http://t.co/UFqdbQcAmk"));
        expectedResults.add(tupleFactory.newTuple("https://t.co/UTohcBofKL"));
        expectedResults.add(tupleFactory.newTuple("http://t.co/mloDH9N6d4"));
        return expectedResults;
    }

}
