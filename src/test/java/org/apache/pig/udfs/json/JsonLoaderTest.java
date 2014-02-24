package org.apache.pig.udfs.json;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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

public class JsonLoaderTest extends TestCase {
	
	private ExecType execType = ExecType.LOCAL;
//    private MiniCluster cluster = MiniCluster.buildCluster();
    private static PigServer pig;
    private static final String datadir = "/data/pig/";
    private PigContext pigContext = new PigContext(execType, new Properties());
    
//    private String INPUT_FILE = datadir + "originput";
    private String INPUT_FILE = datadir + "tweets.json";

    @Before
    public void setUp() throws IOException {
//        pig = new PigServer(execType, cluster.getProperties());
    	pig = new PigServer(execType);
//        createJsonInputFile();
    }

    private void createJsonInputFile() throws IOException {
    	String json = "{\"thing\":\"value\",\"menu\":{\"id\":\"file\",\"value\":\"File\",\"popup\":{\"menuitem\": [{\"value\":323,\"onclick\":\"CreateNewDoc()\"},{\"value\":\"Open\",\"onclick\":\"OpenDoc()\"}, {\"value\":\"Close\",\"onclick\":\"CloseDoc()\"}]}}}";
    	PrintStream out = null;
    	try {
    	    out = new PrintStream(new FileOutputStream(INPUT_FILE));
    	    out.print(json);
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
        pig.registerQuery("a = LOAD '" + INPUT_FILE + "' using epic.colorado.edu.udfs.JsonLoader() " +
                "as (json:map[]);");
        
//        pig.registerQuery("b = foreach a generate flatten(json#'menu') as menu;");
//        pig.registerQuery("c = foreach b generate flatten(menu#'popup') as popup;");
//        pig.registerQuery("d = foreach c generate flatten(popup#'menuitem') as menuitem;");
//        pig.registerQuery("e = foreach d generate flatten(menuitem#'value') as val;");
        
        
        pig.registerQuery("b = foreach a generate FLATTEN(json#'entities') as entities;");
        pig.registerQuery("c = foreach b generate flatten(entities#'urls') as urls;");
        pig.registerQuery("d = foreach c generate flatten(urls#'url') as url;");
////        pig.registerQuery("e = foreach d generate flatten(menuitem#'value') as val;");
        
        
        List<Tuple> expectedResults = buildExpectedNestedJsonResults();
        Iterator<Tuple> iterator = pig.openIterator("d");
        int counter = 0;
        while (iterator.hasNext()) {
//            assertEquals(expectedResults.get(counter++).toString(), iterator.next().toString());
        	System.out.println(iterator.next().toString());
        }
    }

    private List<Tuple> buildExpectedNestedJsonResults() {
        List<Tuple> expectedResults = new LinkedList<Tuple>();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        expectedResults.add(tupleFactory.newTuple("New"));
        expectedResults.add(tupleFactory.newTuple("Open"));
        expectedResults.add(tupleFactory.newTuple("Close"));
        return expectedResults;
    }

}
