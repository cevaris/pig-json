package org.apache.pig.udfs.json;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class JsonToMapTest extends TestCase {
	
	private ExecType execType = ExecType.LOCAL;
    private static PigServer pig;
    private static final String datadir = "/tmp/";
    private PigContext pigContext = new PigContext(execType, new Properties());
    
    private String INPUT_FILE = datadir + "tweets.json";

    @Before
    public void setUp() throws IOException, URISyntaxException {
    	pig = new PigServer(execType);
        createJsonInputFile();
    }

    private void createJsonInputFile() throws IOException, URISyntaxException{
    	InputStream input = getClass().getClassLoader().getResourceAsStream("tweets.json");
    	BufferedReader in = new BufferedReader(new InputStreamReader(input));

    	String line = null;
    	StringBuilder json = new StringBuilder();
    	while((line = in.readLine()) != null) {
    		json.append(line);
    		json.append("\n");
    	}
    	
    	PrintStream out = null;
    	try {
    	    out = new PrintStream(new FileOutputStream(INPUT_FILE));
    	    out.print(json.toString());
    	}
    	finally {
    	    if (out != null) out.close();
    	}
    }

    @After
    public void tearDown() {
        pig.shutdown();
    }

    @Test
    public void test_JsonLoader_Parses_Deeply_Nested_Json_Field() throws IOException {
        pigContext.connect();
        pig.registerQuery("a = LOAD '" + INPUT_FILE + "' AS (text:chararray);");
        pig.registerQuery("b = foreach a generate FLATTEN(org.apache.cassandra.hadoop.pig.json.JsonToMap(text)) as json;");
        pig.registerQuery("c = foreach b generate FLATTEN(json#'entities') as entities;");
        pig.registerQuery("d = foreach c generate flatten(entities#'urls') as urls;");
        pig.registerQuery("e = foreach d generate flatten(urls#'url') as url;");
        
        List<Tuple> expectedResults = buildExpectedNestedJsonResults();
        Iterator<Tuple> iterator = pig.openIterator("e");
        while (iterator.hasNext()) {
        	Tuple expected = expectedResults.remove(0);
        	Tuple current = iterator.next();
            assertEquals(expected.toString(), current.toString());
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
