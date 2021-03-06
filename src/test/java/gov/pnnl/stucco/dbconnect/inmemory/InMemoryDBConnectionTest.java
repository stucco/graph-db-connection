package gov.pnnl.stucco.dbconnect.inmemory;


import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionTestInterface;
import gov.pnnl.stucco.dbconnect.DBConstraint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.json.JSONObject;

/**
 * Unit test for generically Testing the DBConnection
 * NOTE: two environment variable must be defined:
 *       STUCCO_DB_CONFIG=<path/filename.yml>
 *       STUCCO_DB_TYPE= INMEMORY|ORIENTDB|TITAN|NEO4J
 */
public class InMemoryDBConnectionTest 
extends TestCase
{
    private static DBConnectionFactory factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.INMEMORY);
    private static DBConnectionTestInterface conn;

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public InMemoryDBConnectionTest( String testName )
    {
        super( testName );
    }

    public void setUp(){
        //TODO: add this to other tests below
        conn = factory.getDBConnectionTestInterface();
        conn.open();
        //System.out.println(" Global setUp started");
        //System.out.println(" Global setUp done");
    }

    public void tearDown(){
        //System.out.println(" Global tearDown ");
        conn.removeAllVertices();
        conn.close();
    }

    /**
     * get the vertex's property map using the vertex's canonical name
     * @param vertName
     * @return property map
     */
    private List<Map<String,Object>> getVertsByName(String vertName) {
        List<Map<String,Object>> retVal = new LinkedList<Map<String,Object>>();
        List<String> ids = getVertIDsByName(vertName);
        if(ids == null)
            return null;
        for(String currID : ids){
            Map<String, Object> currVert = conn.getVertByID(currID);
            if(currVert == null)
                throw new IllegalStateException("bad state: found vert id with no content.");
            retVal.add(currVert);
        }
        return retVal;
    }

    private Map<String,Object> getVertByName(String vertName){
        return getVertsByName(vertName).get(0);
    }

    /**
     * get the vertexID using the canonical name
     * @param vertName
     * @return ID
     */
    private List<String> getVertIDsByName(String vertName){
        if(vertName == null || vertName == "")
            return null;
        List<DBConstraint> constraints = new ArrayList<DBConstraint>(1);
        DBConstraint c1 = conn.getConstraint("name", Condition.eq, vertName );
        constraints.add( c1 );
        return conn.getVertIDsByConstraints(constraints);
    }

    private String getVertIDByName(String vertName){
        return getVertIDsByName(vertName).get(0);
    }

    /**
     * Tests loading, querying, and other basic operations for vertices, edges, properties.
     */
    public void testLoad()
    {
        String vert1 = "{" +
                "\"name\":\"CVE-1999-0002\"," +
                "\"_type\":\"vertex\","+
                "\"source\":[\"CVE\"],"+
                "\"description\":\"Buffer overflow in NFS mountd gives root access to remote attackers, mostly in Linux systems.\","+
                "\"references\":["+
                    "\"CERT:CA-98.12.mountd\","+
                    "\"http://www.ciac.org/ciac/bulletins/j-006.shtml\","+
                    "\"http://www.securityfocus.com/bid/121\","+
                    "\"XF:linux-mountd-bo\"],"+
                "\"status\":\"Entry\","+
                "\"score\":1.0,"+
                "\"foo\":1"+
                "}";
        String vert2 = "{"+
                "\"name\":\"CVE-1999-nnnn\"," +
                "\"_type\":\"vertex\","+
                "\"source\":[\"CVE\"],"+
                "\"description\":\"test description asdf.\","+
                "\"references\":[\"http://www.google.com\"],"+
                "\"status\":\"Entry\","+
                "\"score\":1.0"+
                "}";
        conn.addVertex(conn.jsonVertToMap(new JSONObject(vert1)));
        conn.addVertex(conn.jsonVertToMap(new JSONObject(vert2)));
        assertEquals(2, conn.getVertCount());
        assertEquals(0, conn.getEdgeCount());

        //find this node, check some properties.
        String id = getVertIDByName("CVE-1999-0002");
        Map<String, Object> vertProps = conn.getVertByID(id);
        String[] expectedStrs = {"CERT:CA-98.12.mountd","XF:linux-mountd-bo","http://www.ciac.org/ciac/bulletins/j-006.shtml","http://www.securityfocus.com/bid/121"};
        Set expectedRefs = new HashSet(Arrays.asList(expectedStrs));
        Set<String> actualRefs = ((Set<String>)vertProps.get("references"));
        assertTrue(actualRefs.equals(expectedRefs));

        //find the other node, check its properties.
        String id2 = getVertIDByName("CVE-1999-nnnn");
        vertProps = (Map<String,Object>)getVertByName("CVE-1999-nnnn");
        assertEquals("test description asdf.", vertProps.get("description"));
        expectedRefs = new HashSet();
        expectedRefs.add("http://www.google.com");
        actualRefs = (Set<String>)vertProps.get("references");
        assertTrue(actualRefs.equals(expectedRefs));

        //There should be no edge between them
        assertEquals(0, conn.getEdgeCountByRelation(id, id2, "sameAs"));
        assertEquals(0, conn.getEdgeCountByRelation(id2, id, "sameAs")); //just to be sure.

        //now add an edge
        //conn.addEdge(inVid, outVid, label);
        conn.addEdge(id, id2, "sameAs");

        //and now we can test the edge between them
        assertEquals(2, conn.getVertCount());
        assertEquals(1, conn.getEdgeCount());
        int c1 = conn.getEdgeCountByRelation(id, id2, "sameAs");
        int c2 = conn.getEdgeCountByRelation(id2, id, "sameAs");
        assertEquals(1, c1);
        assertEquals(0, c2);

        List<String> matchingIDs = null;
        matchingIDs = conn.getInVertIDsByRelation(id2, "sameAs");
        assertEquals(1, matchingIDs.size());
        assertEquals(id, matchingIDs.get(0));
        matchingIDs = conn.getOutVertIDsByRelation(id2, "sameAs");
        assertEquals(0, matchingIDs.size());

        matchingIDs = conn.getOutVertIDsByRelation(id, "sameAs");
        assertEquals(1, matchingIDs.size());
        assertEquals(id2, matchingIDs.get(0));
        matchingIDs = conn.getInVertIDsByRelation(id, "sameAs");
        assertEquals(0, matchingIDs.size());

        matchingIDs = conn.getVertIDsByRelation(id, "sameAs");
        assertEquals(1, matchingIDs.size());
        assertEquals(id2, matchingIDs.get(0));
        matchingIDs = conn.getVertIDsByRelation(id2, "sameAs");
        assertEquals(1, matchingIDs.size());
        assertEquals(id, matchingIDs.get(0));

        List<Map<String,Object>> matchingEdges = null;
        Map<String,Object> matchingEdge = null;
        matchingEdges = conn.getOutEdges(id2);
        assertEquals(1, matchingEdges.size());
        matchingEdge = matchingEdges.get(0);
        assertEquals(id, matchingEdge.get("inVertID"));
        assertEquals(id2, matchingEdge.get("outVertID"));
        assertEquals("sameAs", matchingEdge.get("relation"));
        matchingEdges = conn.getInEdges(id2);
        assertEquals(0, matchingEdges.size());

        matchingEdges = conn.getInEdges(id);
        assertEquals(1, matchingEdges.size());
        matchingEdge = matchingEdges.get(0);
        assertEquals(id, matchingEdge.get("inVertID"));
        assertEquals(id2, matchingEdge.get("outVertID"));
        assertEquals("sameAs", matchingEdge.get("relation"));
        matchingEdges = conn.getOutEdges(id);
        assertEquals(0, matchingEdges.size());

        long vertCount = conn.getVertCount();
        assertEquals(2, vertCount);
        
        long edgeCount = conn.getEdgeCount();
        assertEquals(1, edgeCount);
        
        List<Map<String,Object>> resultsID = conn.getInEdges(id);
        List<Map<String,Object>> resultsID2 = conn.getInEdges(id2); 
        
        assertEquals(1, resultsID.size());
        assertEquals(0, resultsID2.size());
        
        resultsID = conn.getOutEdges(id); 
        resultsID2 = conn.getOutEdges(id2);
        
        assertEquals(0, resultsID.size());
        assertEquals(1, resultsID2.size());
    }

    /**
     * Tests deleting vertices and edges.
     */
    public void testDelete()
    {
        String vert1 = "{" +
                "\"name\":\"CVE-0001-0001\"," +
                "}";
        String vert2 = "{"+
                "\"name\":\"CVE-0001-0002\"," +
                "}";
        String id1 = conn.addVertex(conn.jsonVertToMap(new JSONObject(vert1)));
        String id2 = conn.addVertex(conn.jsonVertToMap(new JSONObject(vert2)));
        String relation = "asdf";
        conn.addEdge(id1, id2, relation);

        //confirm the nodes and edges above
        String id1_t = getVertIDByName("CVE-0001-0001");
        String id2_t = getVertIDByName("CVE-0001-0002");
        assertEquals(id1, id1_t);
        assertEquals(id2, id2_t);

        assertEquals(2, conn.getVertCount());
        assertEquals(1, conn.getEdgeCount());

        assertEquals(1, conn.getOutVertIDsByRelation(id1, relation).size());
        assertEquals(id2, conn.getOutVertIDsByRelation(id1, relation).get(0));
        assertEquals(0, conn.getInVertIDsByRelation(id1, relation).size());

        assertEquals(1, conn.getInVertIDsByRelation(id2, relation).size());
        assertEquals(id1, conn.getInVertIDsByRelation(id2, relation).get(0));
        assertEquals(0, conn.getOutVertIDsByRelation(id2, relation).size());

        //remove edges that we didn't add
        conn.removeEdgeByRelation(id2, id1, relation);
        conn.removeEdgeByRelation(id1, id1, relation);
        conn.removeEdgeByRelation(id2, id2, relation);
        assertEquals(2, conn.getVertCount());
        assertEquals(1, conn.getEdgeCount());

        assertEquals(1, conn.getOutVertIDsByRelation(id1, relation).size());
        assertEquals(id2, conn.getOutVertIDsByRelation(id1, relation).get(0));
        assertEquals(0, conn.getInVertIDsByRelation(id1, relation).size());

        assertEquals(1, conn.getInVertIDsByRelation(id2, relation).size());
        assertEquals(id1, conn.getInVertIDsByRelation(id2, relation).get(0));
        assertEquals(0, conn.getOutVertIDsByRelation(id2, relation).size());

        //now remove the edge
        conn.removeEdgeByRelation(id1, id2, relation);
        assertEquals(2, conn.getVertCount());
        assertEquals(0, conn.getEdgeCount());

        assertEquals(0, conn.getOutVertIDsByRelation(id1, relation).size());
        assertEquals(0, conn.getInVertIDsByRelation(id1, relation).size());
        assertEquals(0, conn.getInVertIDsByRelation(id2, relation).size());
        assertEquals(0, conn.getOutVertIDsByRelation(id2, relation).size());

        assertEquals(2, conn.getVertCount());
        assertEquals(0, conn.getEdgeCount());

        //re-add the edge, and confirm that removing one of its vertices removes the edge.
        conn.addEdge(id1, id2, relation);
        assertEquals(2, conn.getVertCount());
        assertEquals(1, conn.getEdgeCount());

        conn.removeVertByID(id2);
        //confirm vertex was removed
        assertEquals(0, getVertIDsByName("CVE-0001-0002").size());
        assertNull(conn.getVertByID(id2));
        //confirm edge was removed
        assertEquals(0, conn.getOutVertIDsByRelation(id1, relation).size());
        assertEquals(0, conn.getInVertIDsByRelation(id1, relation).size());
        assertEquals(0, conn.getInVertIDsByRelation(id2, relation).size());
        assertEquals(0, conn.getOutVertIDsByRelation(id2, relation).size());

        assertEquals(1, conn.getVertCount());
        assertEquals(0, conn.getEdgeCount());
    }

    /**
     * Tests updating vertex properties
     */
    public void testUpdate()
    {
        String vert1 = "{"+
                "\"endIPInt\":55," +
                "\"_type\":\"vertex\","+
                "\"source\": [\"aaaa\"],"+
                "\"name\":\"testvert_55\"" +
                "}";
        conn.addVertex(conn.jsonVertToMap(new JSONObject(vert1)));
        assertEquals(1, conn.getVertCount());
        assertEquals(0, conn.getEdgeCount());

        //get some properties
        String id = getVertIDByName("testvert_55");
        Map<String, Object> vertProps = conn.getVertByID(id);
        assertEquals( "55", vertProps.get("endIPInt").toString());
        assertEquals( "[aaaa]", vertProps.get("source").toString());
        Map<String, Object> newProps = new HashMap<String, Object>();
        newProps.put("source", new String[] {"aaaa"});
        conn.updateVertex(id, newProps);

        //add a single item to a set-type property - throws an exception
        newProps.put("source", "zzzz");
        boolean thrown = false;
        try {
            conn.updateVertex(id, newProps);
        } catch (IllegalArgumentException e) {
            thrown = true;
        }
        assertTrue(thrown);

        //confirm nothing changed during above.
        vertProps = conn.getVertByID(id);
        assertEquals( "55", vertProps.get("endIPInt").toString());
        assertEquals( "[aaaa]", vertProps.get("source").toString());

        //update an int property, add a new int property
        newProps = new HashMap<String, Object>();
        newProps.put("startIPInt", "33");
        newProps.put("endIPInt", "44");
        conn.updateVertex(id, newProps);
        vertProps = conn.getVertByID(id);
        assertEquals("33", vertProps.get("startIPInt").toString());
        assertEquals("44", vertProps.get("endIPInt").toString());

        //adding a one-item set to the set-type property
        Set<String> tempSet = new HashSet<String>();
        tempSet.add("bbbb");
        newProps.put("source", tempSet);
        conn.updateVertex(id, newProps);
        vertProps = conn.getVertByID(id);
        Set<String> expectedSources = new HashSet<String>( Arrays.asList("aaaa", "bbbb" ) );
        assertEquals(expectedSources, vertProps.get("source"));

        //add a List of things to the set-type property, some of which are redundant
        newProps = new HashMap<String, Object>();
        List<String> sourceList = new ArrayList<String>();
        sourceList.add("aaaa");
        sourceList.add("eeee");
        sourceList.add("aaaa");
        sourceList.add("ffff");
        sourceList.add("aaaa");
        sourceList.add("bbbb");
        newProps.put("source", sourceList);
        conn.updateVertex(id, newProps);
        vertProps = conn.getVertByID(id);
        expectedSources = new HashSet<String>( Arrays.asList("aaaa", "bbbb", "eeee", "ffff" ) );
        assertEquals(expectedSources, vertProps.get("source"));

        //add an Array of things to the set-type property, some of which are redundant
        newProps = new HashMap<String, Object>();
        String[] sourceArr = new String[]{ "aaaa", "aaaa", "aaaa", "eeee", "hhhh", "gggg"};
        newProps.put("source", sourceArr);
        conn.updateVertex(id, newProps);
        vertProps = conn.getVertByID(id);
        expectedSources = new HashSet<String>( Arrays.asList("aaaa", "bbbb", "eeee", "ffff", "gggg", "hhhh" ) );
        assertEquals(expectedSources, vertProps.get("source"));

        assertEquals(1, conn.getVertCount());
        assertEquals(0, conn.getEdgeCount());
    }

    /**
     * creates a vertex of high reverse degree, and one of low degree, and searches for the edge(s) between them.
     */
    public void testHighForwardDegreeVerts()
    {
        String vert1 = "{" +
                "\"name\":\"/usr/local/something\"," +
                "\"_type\":\"vertex\","+
                "\"source\":[\"test\"],"+
                "\"vertexType\":\"software\""+
                "}";
        String vert2 = "{" +
                "\"name\":\"11.11.11.11:1111_to_22.22.22.22:1\"," +
                "\"_type\":\"vertex\","+
                "\"source\":[\"test\"],"+
                "\"vertexType\":\"flow\""+
                "}";
        conn.addVertex(conn.jsonVertToMap(new JSONObject(vert1)));
        conn.addVertex(conn.jsonVertToMap(new JSONObject(vert2)));

        //find node ids
        String id = getVertIDByName("/usr/local/something");
        String id2 = getVertIDByName("11.11.11.11:1111_to_22.22.22.22:1");

        //There should be no edge between them
        assertEquals(0, conn.getEdgeCountByRelation(id, id2, "hasFlow"));
        assertEquals(0, conn.getEdgeCountByRelation(id2, id, "hasFlow"));

        //now add an edge
        //conn.addEdge(inVid, outVid, label);
        conn.addEdge(id2, id, "hasFlow");

        //Confirm the edge between them
        int c1 = conn.getEdgeCountByRelation(id2, id, "hasFlow");
        int c2 = conn.getEdgeCountByRelation(id, id2, "hasFlow");
        assertEquals(1, c1);
        assertEquals(0, c2);

        for(int i=2; i<800; i++){
            String currentVert = "{" +
                    "\"name\":\"11.11.11.11:1111_to_22.22.22.22:" + i + "\"," +
                    "\"_type\":\"vertex\","+
                    "\"source\":[\"test\"],"+
                    "\"vertexType\":\"flow\""+
                    "}";
            String currentId = conn.addVertex(conn.jsonVertToMap(new JSONObject(currentVert)));

            //conn.addEdge(inVid, outVid, label);
            conn.addEdge(currentId, id, "hasFlow");
        }

        //find node ids
        id = getVertIDByName("/usr/local/something");
        id2 = getVertIDByName("11.11.11.11:1111_to_22.22.22.22:1");

        //Confirm the edge between them
        assertEquals(1, conn.getEdgeCountByRelation(id2, id, "hasFlow"));
        assertEquals(0, conn.getEdgeCountByRelation(id, id2, "hasFlow"));
    }

    /**
     * creates a vertex of high reverse degree, and one of low degree, and searches for the edge(s) between them.
     */
    public void testHighReverseDegreeVerts()
    {
        String vert1 = "{" +
                "\"name\":\"11.11.11.11:1111\"," +
                "\"_type\":\"vertex\","+
                "\"source\":[\"test\"],"+
                "\"vertexType\":\"address\""+
                "}";
        String vert2 = "{" +
                "\"name\":\"11.11.11.11\"," +
                "\"_type\":\"vertex\","+
                "\"source\":[\"test\"],"+
                "\"vertexType\":\"IP\""+
                "}";
        conn.addVertex(conn.jsonVertToMap(new JSONObject(vert1)));
        conn.addVertex(conn.jsonVertToMap(new JSONObject(vert2)));

        //find node ids
        String id = getVertIDByName("11.11.11.11:1111");
        String id2 = getVertIDByName("11.11.11.11");

        //There should be no edge between them
        assertEquals(0, conn.getEdgeCountByRelation(id, id2, "hasIP"));
        assertEquals(0, conn.getEdgeCountByRelation(id2, id, "hasIP"));

        //conn.addEdge(inVid, outVid, label);
        conn.addEdge(id2, id, "hasIP");

        //find node ids
        id = getVertIDByName("11.11.11.11:1111");
        id2 = getVertIDByName("11.11.11.11");

        //Confirm the edge between them
        assertEquals(1, conn.getEdgeCountByRelation(id2, id, "hasIP"));
        assertEquals(0, conn.getEdgeCountByRelation(id, id2, "hasIP"));

        for(int i=1200; i<2000; i++){
            String currentVert = "{" +
                    "\"name\":\"11.11.11.11:" + i + "\"," +
                    "\"_type\":\"vertex\","+
                    "\"source\":[\"test\"],"+
                    "\"vertexType\":\"address\""+
                    "}";
            String currentId = conn.addVertex(conn.jsonVertToMap(new JSONObject(currentVert)));

            //conn.addEdge(inVid, outVid, label);
            conn.addEdge(id2, currentId, "hasIP");
        }

        //find node ids
        id = getVertIDByName("11.11.11.11:1111");
        id2 = getVertIDByName("11.11.11.11");

        //Confirm the edge between them
        assertEquals(1, conn.getEdgeCountByRelation(id2, id, "hasIP"));
        assertEquals(0, conn.getEdgeCountByRelation(id, id2, "hasIP"));
    }

    /**
     * creates a small set of vertices, searches this set by constraints on properties
     */
    public void testConstraints()
    {
        Map<String, Object> vert;
        List<DBConstraint> constraints;
        String id;
        List<String> ids;
        List<String> expectedIds;

        vert = new HashMap<String, Object>();
        vert.put("name", "aaa_5");
        vert.put("aaa", 5);
        conn.addVertex(vert);

        vert = new HashMap<String, Object>();
        vert.put("name", "aaa_6");
        vert.put("aaa", 6);
        conn.addVertex(vert);

        vert = new HashMap<String, Object>();
        vert.put("name", "aaa_7");
        vert.put("aaa", 7);
        conn.addVertex(vert);

        DBConstraint c1 = conn.getConstraint("aaa", Condition.eq, new Integer(5) );
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        assertEquals(1, ids.size());
        assertTrue(ids.contains(getVertIDByName("aaa_5")));

        c1 = conn.getConstraint("aaa", Condition.neq, new Integer(5) );
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        expectedIds = new LinkedList<String>();
        expectedIds.add(getVertIDByName("aaa_6"));
        expectedIds.add(getVertIDByName("aaa_7"));
        assertTrue(ids.containsAll(expectedIds));
        assertEquals(2, ids.size());

        c1 = conn.getConstraint("aaa", Condition.gt, new Integer(5) );
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        expectedIds = new LinkedList<String>();
        expectedIds.add(getVertIDByName("aaa_6"));
        expectedIds.add(getVertIDByName("aaa_7"));
        assertTrue(ids.containsAll(expectedIds));
        assertEquals(2, ids.size());

        c1 = conn.getConstraint("aaa", Condition.gte, new Integer(5) );
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        expectedIds = new LinkedList<String>();
        expectedIds.add(getVertIDByName("aaa_5"));
        expectedIds.add(getVertIDByName("aaa_6"));
        expectedIds.add(getVertIDByName("aaa_7"));
        assertTrue(ids.containsAll(expectedIds));
        assertEquals(3, ids.size());

        c1 = conn.getConstraint("aaa", Condition.lt, new Integer(6) );
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        assertTrue(ids.contains(getVertIDByName("aaa_5")));
        assertEquals(1, ids.size());

        c1 = conn.getConstraint("aaa", Condition.lte, new Integer(6) );
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        expectedIds = new LinkedList<String>();
        expectedIds.add(getVertIDByName("aaa_5"));
        expectedIds.add(getVertIDByName("aaa_6"));
        assertTrue(ids.containsAll(expectedIds));
        assertEquals(2, ids.size());

        vert = new HashMap<String, Object>();
        vert.put("name", "bbb_4_5_6");
        HashSet temp3 = new HashSet();
        temp3.add(4);
        temp3.add(5);
        temp3.add(6);
        vert.put("bbb", temp3 );
        String vID = conn.addVertex(vert);

        vert = new HashMap<String, Object>();
        vert.put("name", "bbb_Integer_4_5_6");
        Integer[] temp2 = new Integer[] {new Integer(4), new Integer(5), new Integer(6)};
        vert.put("bbb", new HashSet( Arrays.asList(temp2) ) );
        vID = conn.addVertex(vert);

        vert = new HashMap<String, Object>();
        vert.put("name", "bbb_7_8_9");
        vert.put("bbb", (new int[] {7,8,9}) );
        conn.addVertex(vert);

        vert = new HashMap<String, Object>();
        vert.put("name", "bbb_5_6_7_8");
        vert.put("bbb", (new int[] {5,6,7,8}) );
        conn.addVertex(vert);

        vert = new HashMap<String, Object>();
        vert.put("name", "bbb_asdf");
        vert.put("bbb", "asdf" );
        conn.addVertex(vert);

        vert = new HashMap<String, Object>();
        vert.put("name", "bbb_asdf4.222");
        vert.put("bbb", "asdf4.222" );
        conn.addVertex(vert);

        vert = new HashMap<String, Object>();
        vert.put("name", "bbb_55");
        vert.put("bbb", 55 );
        conn.addVertex(vert);

        vert = new HashMap<String, Object>();
        vert.put("name", "bbb_101_102_103");
        //vert.put("bbb", (new double[] {101.0, 102.0, 103.0}) );
        //TODO: above should work, but set conversion not added yet, see convertMultiValueToSet in DBConnectionBase.java
        Double[] temp = new Double[] {101.0, 102.0, 103.0};
        vert.put("bbb", new HashSet( Arrays.asList(temp) ) );
        conn.addVertex(vert);

        c1 = conn.getConstraint("bbb", Condition.contains, 4);
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        expectedIds = new LinkedList<String>();
        expectedIds.add(getVertIDByName("bbb_4_5_6"));
        expectedIds.add(getVertIDByName("bbb_Integer_4_5_6"));
        assertTrue(ids.containsAll(expectedIds));
        assertEquals(2, ids.size());

        c1 = conn.getConstraint("bbb", Condition.contains, new Integer(4));
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        expectedIds = new LinkedList<String>();
        expectedIds.add(getVertIDByName("bbb_4_5_6"));
        expectedIds.add(getVertIDByName("bbb_Integer_4_5_6"));
        assertTrue(ids.containsAll(expectedIds));
        assertEquals(2, ids.size());

        c1 = conn.getConstraint("bbb", Condition.contains, 4.0);
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        assertEquals(0, ids.size());

        c1 = conn.getConstraint("bbb", Condition.contains, 4.2);
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        assertEquals(0, ids.size());

        c1 = conn.getConstraint("bbb", Condition.contains, 5);
        DBConstraint c2 = conn.getConstraint("bbb", Condition.contains, 7);
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        constraints.add(c2);
        ids = conn.getVertIDsByConstraints(constraints);
        expectedIds = new LinkedList<String>();
        expectedIds.add(getVertIDByName("bbb_5_6_7_8"));
        assertTrue(ids.containsAll(expectedIds));
        assertEquals(1, ids.size());

        c1 = conn.getConstraint("bbb", Condition.substring, "asdf");
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        expectedIds = new LinkedList<String>();
        expectedIds.add(getVertIDByName("bbb_asdf"));
        expectedIds.add(getVertIDByName("bbb_asdf4.222"));
        assertTrue(ids.containsAll(expectedIds));
        assertEquals(2, ids.size());

        c1 = conn.getConstraint("bbb", Condition.substring, "as");
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        expectedIds = new LinkedList<String>();
        expectedIds.add(getVertIDByName("bbb_asdf"));
        expectedIds.add(getVertIDByName("bbb_asdf4.222"));
        assertTrue(ids.containsAll(expectedIds));
        assertEquals(2, ids.size());

        c1 = conn.getConstraint("bbb", Condition.contains, 103);
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        assertEquals(0, ids.size());

        c1 = conn.getConstraint("bbb", Condition.contains, 103.0);
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);
        ids = conn.getVertIDsByConstraints(constraints);
        expectedIds = new LinkedList<String>();
        expectedIds.add(getVertIDByName("bbb_101_102_103"));
        assertTrue(ids.containsAll(expectedIds));
        assertEquals(1, ids.size());
    }

    /**
     * creates a small set of vertices, searches neighboring vertices by constraints on properties
     */
    public void testRelationConstraints()
    {
        //InMemoryDBConnection conn = new InMemoryDBConnection();
        Map<String, Object> vert;
        List<DBConstraint> constraints;
        List<String> ids;
        List<String> expectedIds;

        vert = new HashMap<String, Object>();
        vert.put("name", "center");
        String id_center = conn.addVertex(vert);

        vert = new HashMap<String, Object>();
        vert.put("name", "aaa_5");
        vert.put("aaa", 5);
        String id_aaa_5 = conn.addVertex(vert);
        conn.addEdge(id_aaa_5, id_center, "r");

        vert = new HashMap<String, Object>();
        vert.put("name", "aaa_6");
        vert.put("aaa", 6);
        String id_aaa_6 = conn.addVertex(vert);
        conn.addEdge(id_aaa_6, id_center, "r");

        vert = new HashMap<String, Object>();
        vert.put("name", "aaa_7");
        vert.put("aaa", 7);
        String id_aaa_7 = conn.addVertex(vert);
        conn.addEdge(id_aaa_7, id_center, "r");

        DBConstraint c1 = new InMemoryConstraint("aaa", Condition.eq, new Integer(5) );
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);

        ids = conn.getInVertIDsByRelation(id_center, "r", constraints);
        assertEquals(1, ids.size());
        assertTrue(ids.contains(getVertIDByName("aaa_5")));
        ids = conn.getOutVertIDsByRelation(id_center, "r", constraints);
        assertEquals(0, ids.size());

        List<DBConstraint> emptyConstraints = new LinkedList<DBConstraint>();
        ids = conn.getOutVertIDsByRelation(id_aaa_5, "r", emptyConstraints);
        assertEquals(1, ids.size());
        assertEquals(id_center, ids.get(0));
        ids = conn.getInVertIDsByRelation(id_aaa_5, "r", emptyConstraints);
        assertEquals(0, ids.size());

        ids = conn.getVertIDsByRelation(id_center, "r", constraints);
        assertEquals(1, ids.size());
        assertEquals(getVertIDByName("aaa_5"), ids.get(0));

        c1 = new InMemoryConstraint("aaa", Condition.neq, new Integer(5) );
        constraints = new LinkedList<DBConstraint>();
        constraints.add(c1);

        ids = conn.getInVertIDsByRelation(id_center, "r", constraints);
        assertEquals(2, ids.size());
        assertTrue(ids.contains(id_aaa_6));
        assertTrue(ids.contains(id_aaa_7));
        ids = conn.getOutVertIDsByRelation(id_center, "r", constraints);
        assertEquals(0, ids.size());

        ids = conn.getVertIDsByRelation(id_center, "r", constraints);
        assertEquals(2, ids.size());
        assertTrue(ids.contains(id_aaa_6));
        assertTrue(ids.contains(id_aaa_7));
    }
}


