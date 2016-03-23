package gov.pnnl.stucco.dbconnect.titan;


import static org.junit.Assert.*;
import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionIndexerInterface;
import gov.pnnl.stucco.dbconnect.DBConnectionTestInterface;
import gov.pnnl.stucco.dbconnect.DBConstraint;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

//import junit.framework.TestCase;



import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.tinkerpop.rexster.client.RexProException;


/**
 * NOTE: REQUIREMENTS TO RUN THIS TEST:
 * 1) There is only one(1) test that will be run and it can only be run once/per Titan instantiation --  Once you run this test
 *      You must restart the Titan inmemory instance or Titan will throw exceptions claiming (rightfully so)
 *      that you have already defined the properties.  In Titan 0.5.* you cannot through rexster remove properties
 *      once they have been defined or change their type (so you better get them right)
 * 2) The option with the rexster config (inmemory) file will requires you comment out the following option:
 *          <schema.default>none</schema.default>
 *          Typically we use this option to enforce the schema, however, for the test we first
 *          create several vertices and test how long it takes to query for them
 *          The second half of the text creates the properties and indexes
 *          and then tests how long it takes to get them. 
 *          
 * NOTABLE NOTES:
 * 1) getPropertyNamesFromDB() and dropAllIndexes() are no-ops because the current version of titan
 *    requires manually drops of mixed indices and we could find no option to get list of all property
 *    names.  You have the capability to test for the presence of a known property name, but there is 
 *    no option (we could find) that allows you retrieve a list of all defined properties.  If Titan 1.0
 *    eventually has those option then the following calls will be made to do the appropriate thing.
 * 
 * Unit test for generically Testing the DBConnection
 * NOTE: two environment variable must be defined:
 *       STUCCO_DB_CONFIG=<path/filename.yml>
 *       STUCCO_DB_TYPE= INMEMORY|ORIENTDB|TITAN|NEO4J
 *       STUCCO_DB_INDEX_CONFIG=<path/filename.json>
 */

public class DBConnectionIndexerTest 
//extends TestCase
{
    private static DBConnectionFactory factory;// = DBConnectionFactory.getFactory(DBConnectionFactory.Type.INMEMORY);
    private static DBConnectionTestInterface conn;
    private static IndexDefinitionsToTitanGremlin loader;
    private static String indexConfig;

    static {
        // we don't need to get the environment variable
        factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.TITAN);

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
        loader = new IndexDefinitionsToTitanGremlin();
        
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
     * [CAN"T be run in TITAN until the restriction for removing properties and indices are solved]
     */
//    @Test
//    public void testIndexLoad()
//    {
//
//        // load the schema and create the indexes
//        // if any properties are defined or indexes we will get an error if try to redefined them.
//        try {
//            loader.parse(new File(indexConfig));
//            
//         } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        
//        // get the contents from DB regarding which properties are in defined
//
////        try {
////            Set<String> dbPropNames = loader.getPropertyNamesFromDB(); // names retrieved from the DB
//            Set<String> propNames   = loader.getPropertyNames(); // these are the names from which we loaded from a file
////            assertEquals(dbPropNames.size(), propNames.size());
////        } catch (RexProException e) {
////            // Intercept error to print debug info
////            e.printStackTrace();
////            // Re-throw up the stack
////            throw e;
////        }
//    }

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


