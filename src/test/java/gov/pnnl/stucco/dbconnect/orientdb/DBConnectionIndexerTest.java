package gov.pnnl.stucco.dbconnect.orientdb;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionTestInterface;
import gov.pnnl.stucco.dbconnect.DBConstraint;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
//import junit.framework.TestCase;
import org.json.JSONObject;

/**
 * Unit test for generically Testing the DBConnection
 * NOTE: two environment variable must be defined:
 *       STUCCO_DB_CONFIG=<path/filename.yml>
 *       STUCCO_DB_TYPE= INMEMORY|ORIENTDB|TITAN|NEO4J
 *       STUCCO_DB_INDEX_CONFIG=<path/filename.json>
 */

public class DBConnectionIndexerTest 
//extends TestCase
{
    private static DBConnectionFactory factory;
    private static DBConnectionTestInterface conn;
    private static IndexDefinitionsToOrientDB loader;
    private static String indexConfig;

    static {
        // we don't need to get the environment variable
        factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.ORIENTDB);

        String config = System.getenv("STUCCO_DB_CONFIG");
        if (config == null) {
            throw (new NullPointerException("Missing environment variable STUCCO_DB_CONFIG"));
        }
        factory.setConfiguration(config);
        
        indexConfig = System.getenv("STUCCO_DB_INDEX_CONFIG");
        if (indexConfig == null) {
            throw (new NullPointerException("Missing environment variable STUCCO_DB_INDEX_CONFIG"));
        }
        
        conn = factory.getDBConnectionTestInterface();
        conn.open();
        
        // the database is open and ready to go
        loader = new IndexDefinitionsToOrientDB();
        
        loader.setDBConnection(conn);

    }
    

    // 4) add a bunch of data (TBD) and query how long it take to get it
    //    Then create an index and verify that it takes less to get the same data
    

    @Before
    public void setUp(){
        // make sure we have no indexes
        try {
            loader.dropAllIndexes(new File (indexConfig));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @After
    public void tearDown(){
        if (conn != null) {
            conn.removeAllVertices();
        }
    }
    
    @BeforeClass
    public static void setupClass() throws Exception {
        if (conn != null) {
            conn.removeAllVertices();
        }
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
     // make sure we have no indexes
        try {
            loader.dropAllIndexes(new File (indexConfig));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        if (conn != null) {
            conn.removeAllVertices();
            conn.close();
        }
        
    }

    /**
     * Tests loading of index and verifying data is there
     */
    @Test
    public void testIndexLoad()
    {

        // load the schema and create the indexes
        // if any properties are defined or indexes we will get an error if try to redefined them.
        try {
            loader.parse(new File(indexConfig));
            
         } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        // get the contents from DB regarding which properties are in defined
        // use the following query to get a list of property names
        // select field from (select expand(indexDefinition) from (select expand(indexes) from metadata:indexmanager)) 
        // SELECT count FROM (SELECT count(*) FROM (SELECT expand(indexes) FROM metadata:indexmanager ))"; // count of indexes

        try {
            Set<String> dbPropNames = loader.getPropertyNamesFromDB(); // names retrieved from the DB
            Set<String> propNames   = loader.getPropertyNames(); // these are the names from which we loaded from a file
            assertEquals(dbPropNames.size(), propNames.size());
        } catch (OCommandExecutionException e) {
            // Intercept error to print debug info
            e.printStackTrace();
            // Re-throw up the stack
            throw e;
        }
    }

    /**
     * Tests whether adding the index speed up a query
     */
    @Test
    public void testSpeedup()
    {


        long deltaWithoutIndex = timingVertexGet();
        
        // make sure we have no indexes
        try {
            conn.removeAllVertices();
            loader.parse(new File(indexConfig));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        long deltaWithIndex = timingVertexGet();
        
        double ratio = (double)deltaWithoutIndex / (double)deltaWithIndex;
        
        // we assume that indexing will really help
        assertTrue(ratio > 5);
    }
    
    /**
     * add 1000 vertices and time how long it takes to retrieve 100 of them
     * @return time in milliseconds
     */
    private long timingVertexGet() {
        
        // add 1000 vertices
        addVertices();
        
        // query time for get back
        DBConstraint c1 = conn.getConstraint("vertexType", Condition.eq, new String("flow_5") );
        List<DBConstraint> constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        
        // benchmark
        long startTS = System.currentTimeMillis();
        
        List<String> ids = conn.getVertIDsByConstraints(constraints);
        
        long lastTS = System.currentTimeMillis();
        long delta = lastTS-startTS;
        
        assertEquals(100, ids.size());
       
        return delta;
    }
    
    /**
     * add 1000 vertices
     */
    private void addVertices() {

        for(int i=0; i<1000; i++){
            String currentVert = "{" +
                    "\"name\":\"11.11.11.11:1111_to_22.22.22.22:" + i + "\"," +
                    "\"_type\":\"vertex\","+
                    "\"source\":\"test\","+
                    "\"vertexType\":\"flow_"+ i%10 + "\"" + 
                    "}";
            conn.addVertex(conn.jsonVertToMap(new JSONObject(currentVert)));
        }
    }


}


