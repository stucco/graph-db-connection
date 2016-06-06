package gov.pnnl.stucco.dbconnect.postgresql;


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
import java.util.Objects;
import java.util.Collection;
 
import junit.framework.TestCase; 

import org.junit.After; 
import org.junit.AfterClass;
import org.junit.Before; 
import org.junit.BeforeClass;
import org.junit.Test;  
 
import org.json.JSONObject;
import org.json.JSONArray;

/** 
 * Unit test for generically Testing the DBConnection
 * NOTE: two environment variable must be defined:
 *       STUCCO_DB_CONFIG=<path/filename.yml>
 *       STUCCO_DB_TYPE= INMEMORY|ORIENTDB|TITAN|NEO4J|POSTGRESQL
 */ 
public class PostgresqlDBConnectionTest extends TestCase {
    private static DBConnectionFactory factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.POSTGRESQL);
    private static DBConnectionTestInterface conn;

    public void setUp() {
        factory.setConfiguration("./config/postgresql.yml");
        conn = factory.getDBConnectionTestInterface();
        conn.open();
        conn.removeAllVertices();
        conn.buildIndex(null);
    }

    public void tearDown(){
    //    conn.removeAllVertices();
        conn.close();
    }

    public void testLoadOneVertex() {
        String vertexString = 
            "{" +
            "  \"sourceDocument\": \"<ttp:TTP xmlns:ttp=\\\"http://stix.mitre.org/TTP-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:malware-2cbe5820-572c-493f-8008-7cb7bf344dc3\\\" xsi:type=\\\"ttp:TTPType\\\"><ttp:Title>Malware<\\/ttp:Title><ttp:Behavior><ttp:Malware><ttp:Malware_Instance id=\\\"stucco:malware-scanner\\\"><ttp:Type>Scanner<\\/ttp:Type><ttp:Name>Scanner<\\/ttp:Name><ttp:Name>Scanner Alias<\\/ttp:Name><ttp:Name>Scanner Name<\\/ttp:Name><ttp:Title>Scanner Description<\\/ttp:Title><ttp:Description>Scanner Description2<\\/ttp:Description><\\/ttp:Malware_Instance><\\/ttp:Malware><\\/ttp:Behavior><ttp:Resources><ttp:Infrastructure><ttp:Observable_Characterization cybox_major_version=\\\"2.0\\\" cybox_minor_version=\\\"1.0\\\"><cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" idref=\\\"Observable-ef0e7868-0d1f-4f56-ab90-b8ecfea62229\\\" /><\\/ttp:Observable_Characterization><\\/ttp:Infrastructure><\\/ttp:Resources><ttp:Information_Source><stixCommon:Contributing_Sources xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\"><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><\\/stixCommon:Contributing_Sources><\\/ttp:Information_Source><\\/ttp:TTP>\", " +
            "  \"vertexType\": \"Malware\", " +
            "  \"name\": \"Scanner\", " +
            "  \"description\": [\"Scanner Description2\"], " +
            "  \"alias\": [ " +
            "    \"Scanner Name\", " +
            "    \"Scanner Alias\" " +
            "  ], " +
            "  \"source\": [\"Source\"] " +
            "}";

        conn.removeAllVertices();
        Map<String, Object> vertex = conn.jsonVertToMap(new JSONObject(vertexString));
        String id = conn.addVertex(vertex);
        Map<String, Object> expectedVertex = conn.getVertByID(id);
        assertEquals(vertex, expectedVertex);
        long count = conn.getVertCount();
        assertEquals(count, 1);
        count = conn.getEdgeCount();
        assertEquals(count, 0);
    }

    public void testLoadTwoVerticesOneEdge() {
        String graphString =
            "{"+
            "  \"vertices\": {"+
            "    \"Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402\": {"+
            "      \"vertexType\": \"Indicator\","+
            "      \"sourceDocument\": \"<stix:Indicator xmlns:stix=\\\"http://stix.mitre.org/stix-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"indicator:IndicatorType\\\" id=\\\"Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402\\\"><indicator:Title xmlns:indicator=\\\"http://stix.mitre.org/Indicator-2\\\">Indicator One Title<\\/indicator:Title><indicator:Indicated_TTP xmlns:indicator=\\\"http://stix.mitre.org/Indicator-2\\\"><stixCommon:TTP xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xsi:type=\\\"ttp:TTPType\\\" idref=\\\"TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70\\\" /><\\/indicator:Indicated_TTP><\\/stix:Indicator>\","+
            "      \"name\": \"Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402\","+
            "       \"alias\": [\"TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70\"]" +
            "    },"+
            "    \"TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70\": {"+
            "      \"vertexType\": \"TTP\","+
            "      \"sourceDocument\": \"<stix:TTP xmlns:stix=\\\"http://stix.mitre.org/stix-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"ttp:TTPType\\\" id=\\\"TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70\\\"><ttp:Title xmlns:ttp=\\\"http://stix.mitre.org/TTP-1\\\">Related TTP<\\/ttp:Title><\\/stix:TTP>\","+
            "      \"name\": \"TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70\""+
            "    }"+
            "  },"+
            "  \"edges\": ["+
            "    {"+
            "      \"outVertID\": \"Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402\","+
            "      \"inVertID\": \"TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70\","+
            "      \"relation\": \"IndicatedTTP\""+
            "    }"+
            "  ]"+
            "}";

        JSONObject graph = new JSONObject(graphString);
        JSONObject vertices = graph.getJSONObject("vertices");
            
        conn.removeAllVertices();
        Map<String, Object> indicatorVertex = conn.jsonVertToMap(vertices.getJSONObject("Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402")); 
        String outVertID = conn.addVertex(indicatorVertex);
        Map<String, Object> expectedIndicatorVertex = conn.getVertByID(outVertID);
        assertEquals(indicatorVertex, expectedIndicatorVertex);

        Map<String, Object> ttpVertex = conn.jsonVertToMap(vertices.getJSONObject("TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70"));
        String inVertID = conn.addVertex(ttpVertex);
        Map<String, Object> expectedTTPVertex = conn.getVertByID(inVertID);
        assertEquals(ttpVertex, expectedTTPVertex);

        conn.addEdge(inVertID, outVertID, "IndicatedTTP");
        List<Map<String, Object>> outEdges = conn.getOutEdges(outVertID);
        assertEquals(outEdges.size(), 1);
        Map<String, Object> outEdge = outEdges.get(0);
        assertEquals(outEdge.get("outVertID"), outVertID);
        assertEquals(outEdge.get("inVertID"), inVertID);
        assertEquals(outEdge.get("relation"), "IndicatedTTP");
        List<Map<String, Object>> inEdges = conn.getInEdges(inVertID);
        assertEquals(inEdges.size(), 1);
        assertEquals(outEdges, inEdges);

        long vertCount = conn.getVertCount();
        assertEquals(vertCount, 2);
        long edgeCount = conn.getEdgeCount();
        assertEquals(edgeCount, 1);
        edgeCount = 0;
        edgeCount = conn.getEdgeCountByRelation(inVertID, outVertID, "IndicatedTTP");
        assertEquals(edgeCount, 1);

        conn.removeVertByID(outVertID);
        vertCount = conn.getVertCount();
        assertEquals(vertCount, 1);
        edgeCount = conn.getEdgeCount();
        assertEquals(edgeCount, 0);

        conn.removeVertByID(inVertID);
        vertCount = conn.getVertCount();
        assertEquals(vertCount, 0);

        conn.removeAllVertices();
    }

    public void testUpdateProperties() {
        String vertString =
            "{"+
            "      \"endIP\": \"216.98.188.255\","+
            "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" id=\\\"stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe\\\"><cybox:Title>AddressRange<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">CAIDA<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:addressRange-3630349312-3630349567\\\"><cybox:Description>216.98.188.0 through 216.98.188.255<\\/cybox:Description><cybox:Properties xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\" apply_condition=\\\"ANY\\\" condition=\\\"InclusiveBetween\\\" delimiter=\\\" - \\\">216.98.188.0 - 216.98.188.255<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\","+
            "      \"vertexType\": \"AddressRange\","+
            "      \"startIP\": \"216.98.188.0\","+
            "      \"startIPInt\": 3630349312,"+
            "      \"name\": \"216.98.188.0 - 216.98.188.255\","+
            "      \"source\": [\"CAIDA\"],"+
            "      \"endIPInt\": 3630349567,"+
            "      \"observableType\": \"Address\""+
            "}";

        String vertUpdatedString = 
            "{"+
            "      \"endIP\": \"216.98.188.255\","+
            "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" id=\\\"stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe\\\"><cybox:Title>AddressRange<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">CAIDA<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:addressRange-3630349312-3630349567\\\"><cybox:Description>216.98.188.0 through 216.98.188.255<\\/cybox:Description><cybox:Properties xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\" apply_condition=\\\"ANY\\\" condition=\\\"InclusiveBetween\\\" delimiter=\\\" - \\\">216.98.188.0 - 216.98.188.255<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\","+
            "      \"vertexType\": \"AddressRange\","+
            "      \"startIP\": \"216.98.188.0\","+
            "      \"startIPInt\": 3630349312,"+
            "      \"name\": \"216.98.188.0 - 216.98.188.255\","+
            "      \"description\": [\"Range of addresses from 216.98.188.0 through 216.98.188.255\"],"+
            "      \"source\": [\"GeoIP\"],"+
            "      \"endIPInt\": 3630349567,"+
            "      \"observableType\": \"Address\""+
            "}";

        conn.removeAllVertices();
        Map<String, Object> vert = conn.jsonVertToMap(new JSONObject(vertString));
        String id = conn.addVertex(vert);
        Map<String, Object> dbVert = conn.getVertByID(id);
        assertEquals(vert, dbVert);

        Map<String, Object> vertUpdate = conn.jsonVertToMap(new JSONObject(vertUpdatedString));
        conn.updateVertex(id, vertUpdate);
        Map<String, Object> dbVertUpdate = conn.getVertByID(id);
        assertEquals(vertUpdate, dbVertUpdate);
    }

    public void testGetVertIDsByRelation() {
        String graphString = 
            "{"+
            "  \"vertices\": {"+
            "    \"TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341\": {"+
            "      \"vertexType\": \"TTP\","+
            "      \"sourceDocument\": \"<stix:TTP xmlns:stix=\\\"http://stix.mitre.org/stix-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"ttp:TTPType\\\" id=\\\"TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341\\\"><ttp:Title xmlns:ttp=\\\"http://stix.mitre.org/TTP-1\\\">Related TTP<\\/ttp:Title><\\/stix:TTP>\","+
            "      \"name\": \"TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341\""+
            "    },"+
            "    \"Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb\": {"+
            "      \"vertexType\": \"Course_Of_Action\","+
            "      \"sourceDocument\": \"<stix:Course_Of_Action xmlns:stix=\\\"http://stix.mitre.org/stix-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"coa:CourseOfActionType\\\" id=\\\"Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb\\\"><coa:Title xmlns:coa=\\\"http://stix.mitre.org/CourseOfAction-1\\\">COA Title<\\/coa:Title><\\/stix:Course_Of_Action>\","+
            "      \"name\": \"Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb\""+
            "    },"+
            "    \"Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb\": {"+
            "      \"vertexType\": \"Indicator\","+
            "      \"sourceDocument\": \"<stix:Indicator xmlns:stix=\\\"http://stix.mitre.org/stix-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"indicator:IndicatorType\\\" id=\\\"Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb\\\"><indicator:Title xmlns:indicator=\\\"http://stix.mitre.org/Indicator-2\\\">Indicator One Title<\\/indicator:Title><indicator:Indicated_TTP xmlns:indicator=\\\"http://stix.mitre.org/Indicator-2\\\"><stixCommon:TTP xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xsi:type=\\\"ttp:TTPType\\\" idref=\\\"TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341\\\" /><\\/indicator:Indicated_TTP><indicator:Suggested_COAs xmlns:indicator=\\\"http://stix.mitre.org/Indicator-2\\\"><indicator:Suggested_COA><stixCommon:Course_Of_Action xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xsi:type=\\\"coa:CourseOfActionType\\\" idref=\\\"Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb\\\" /><\\/indicator:Suggested_COA><\\/indicator:Suggested_COAs><\\/stix:Indicator>\","+
            "      \"name\": \"Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb\","+
            "       \"alias\": [" +
            "           \"TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341\"," +
            "           \"Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb\"]" +
            "    }"+
            "  },"+
            "  \"edges\": ["+
            "    {"+
            "      \"outVertID\": \"Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb\","+
            "      \"inVertID\": \"TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341\","+
            "      \"relation\": \"IndicatedTTP\""+
            "    },"+
            "    {"+
            "      \"outVertID\": \"Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb\","+
            "      \"inVertID\": \"Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb\","+
            "      \"relation\": \"SuggestedCOA\""+
            "    }"+
            "  ]"+
            "}";

        JSONObject verts = new JSONObject(graphString).getJSONObject("vertices");
        Map<String, Object> indicatorMap = conn.jsonVertToMap(verts.getJSONObject("Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb"));
        String indicatorID = conn.addVertex(indicatorMap);
        Map<String, Object> indicatorMapDB = conn.getVertByID(indicatorID);
        assertEquals(indicatorMap, indicatorMapDB);

        Map<String, Object> ttpMap = conn.jsonVertToMap(verts.getJSONObject("TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341"));
        String ttpID = conn.addVertex(ttpMap);
        Map<String, Object> ttpMapDB = conn.getVertByID(ttpID);
        assertEquals(ttpMap, ttpMapDB);

        Map<String, Object> coaMap = conn.jsonVertToMap(verts.getJSONObject("Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb"));
        String coaID = conn.addVertex(coaMap);
        Map<String, Object> coaMapDB = conn.getVertByID(coaID);
        assertEquals(coaMap, coaMapDB);

        conn.addEdge(ttpID, indicatorID, "IndicatedTTP");
        conn.addEdge(coaID, indicatorID, "SuggestedCOA");

        List<String> inVertIDs = conn.getInVertIDsByRelation(indicatorID, "IndicatedTTP");
        assertEquals(inVertIDs.size(), 1);
        assertEquals(inVertIDs.get(0), ttpID);

        inVertIDs = conn.getInVertIDsByRelation(indicatorID, "SuggestedCOA");
        assertEquals(inVertIDs.size(), 1);
        assertEquals(inVertIDs.get(0), coaID);

        List<String> vertIDs = conn.getVertIDsByRelation(indicatorID, "IndicatedTTP");
        assertEquals(vertIDs.size(), 1);
        ttpMapDB = conn.getVertByID(vertIDs.get(0));
        assertEquals(ttpMap, ttpMapDB);

        vertIDs = conn.getVertIDsByRelation(indicatorID, "SuggestedCOA");
        assertEquals(vertIDs.size(), 1);
        coaMapDB = conn.getVertByID(vertIDs.get(0));
        assertEquals(coaMap, coaMapDB);

        conn.removeAllVertices();
    }

    public void testConstraints() {
        String addrRangeString =
        "{" +
        "      \"endIP\": \"216.98.188.255\"," +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" id=\\\"stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe\\\"><cybox:Title>AddressRange<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">CAIDA<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:addressRange-3630349312-3630349567\\\"><cybox:Description>216.98.188.0 through 216.98.188.255<\\/cybox:Description><cybox:Properties xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\" apply_condition=\\\"ANY\\\" condition=\\\"InclusiveBetween\\\" delimiter=\\\" - \\\">216.98.188.0 - 216.98.188.255<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\","+
        "      \"vertexType\": \"AddressRange\"," +
        "      \"startIP\": \"216.98.188.0\"," +
        "      \"startIPInt\": 3630349312," +
        "      \"name\": \"216.98.188.0 - 216.98.188.255\"," +
        "      \"description\": [\"216.98.188.0 through 216.98.188.255\"]," +
        "      \"source\": [\"CAIDA\"]," +
        "      \"endIPInt\": 3630349567," +
        "      \"observableType\": \"Address\"" +
        "}";

        String ipString =
        "{" +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" id=\\\"stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241\\\"><cybox:Title>IP<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">LoginEvent<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:ip-3232238091\\\"><cybox:Description>192.168.10.11<\\/cybox:Description><cybox:Properties xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\">216.98.188.1<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\","+
        "      \"vertexType\": \"IP\"," +
        "      \"ipInt\": 3630349313," +
        "      \"name\": \"216.98.188.1\"," +
        "      \"description\": [\"216.98.188.1\", \"Some other description.\"]," +
        "      \"source\": [\"LoginEvent\", \"maxmind\"]," +
        "      \"observableType\": \"Address\"" + 
        "}";

        try {
            Map<String, Object> addrRangeMap = conn.jsonVertToMap(new JSONObject(addrRangeString));
            String addrRangeID = conn.addVertex(addrRangeMap);
            assertEquals(addrRangeMap, conn.getVertByID(addrRangeID));

            Map<String, Object> ipMap = conn.jsonVertToMap(new JSONObject(ipString));
            String ipID = conn.addVertex(ipMap);
            assertEquals(ipMap, conn.getVertByID(ipID));

            conn.addEdge(ipID, addrRangeID, "Contained_Within");

            conn.addEdge(addrRangeID, ipID, "Contained_Within");
            Map<String, Object> inEdges = conn.getInEdges(addrRangeID).get(0);
            Map<String, Object> outEdges = conn.getOutEdges(ipID).get(0);
            assertEquals(inEdges, outEdges);

            List<DBConstraint> constraints = new ArrayList<DBConstraint>();
            constraints.add(conn.getConstraint("vertexType", Condition.eq, "IP"));
            constraints.add(conn.getConstraint("ipInt", Condition.eq, 3630349313L));
            String vertID = conn.getVertIDsByConstraints(constraints).get(0);
            Map<String, Object> vert = conn.getVertByID(vertID);
            assertEquals(ipMap, vert);            

            constraints = new ArrayList<DBConstraint>();
            constraints.add(conn.getConstraint("startIPInt", Condition.gte, 3630349312L));
            constraints.add(conn.getConstraint("endIPInt", Condition.lte, 3630349567L));
            String id = conn.getVertIDsByConstraints(constraints).get(0);
            vert = conn.getVertByID(id);
            assertEquals(addrRangeMap, vert);            

            constraints = new ArrayList<DBConstraint>();
            constraints.add(conn.getConstraint("name", Condition.neq, "name"));
            List<String> vertIDsList = conn.getVertIDsByConstraints(constraints);
            assertEquals(vertIDsList.size(), 2);

            constraints = new ArrayList<DBConstraint>();
            constraints.add(conn.getConstraint("source", Condition.contains, "maxmind"));
            vertIDsList = conn.getVertIDsByConstraints(constraints);
            assertEquals(vertIDsList.size(), 1);
            assertEquals(ipMap, conn.getVertByID(vertIDsList.get(0)));

            constraints = new ArrayList<DBConstraint>();
            constraints.add(conn.getConstraint("description", Condition.contains, "Some other description."));
            vertIDsList = conn.getVertIDsByConstraints(constraints);
            assertEquals(vertIDsList.size(), 1);
            assertEquals(ipMap, conn.getVertByID(vertIDsList.get(0)));

            constraints = new ArrayList<DBConstraint>();
            constraints.add(conn.getConstraint("sourceDocument", Condition.substring, "216.98.188.0 - 216.98.188.255"));
            vertIDsList = conn.getVertIDsByConstraints(constraints);
            assertEquals(vertIDsList.size(), 1);
            assertEquals(addrRangeMap, conn.getVertByID(vertIDsList.get(0)));

            constraints = new ArrayList<DBConstraint>();
            constraints.add(conn.getConstraint("sourceDocument", Condition.substring, "CAIDA"));
            vertIDsList = conn.getVertIDsByRelation(ipID, "Contained_Within", constraints);
            assertEquals(vertIDsList.size(), 1);
            assertEquals(addrRangeMap, conn.getVertByID(vertIDsList.get(0)));

            constraints = new ArrayList<DBConstraint>();
            constraints.add(conn.getConstraint("sourceDocument", Condition.substring, "LoginEvent"));
            vertIDsList = conn.getOutVertIDsByRelation(addrRangeID, "Contained_Within", constraints);
            assertEquals(vertIDsList.size(), 1);
            assertEquals(ipMap, conn.getVertByID(vertIDsList.get(0)));

            constraints = new ArrayList<DBConstraint>();
            constraints.add(conn.getConstraint("sourceDocument", Condition.substring, "CAIDA"));
            vertIDsList = conn.getInVertIDsByRelation(ipID, "Contained_Within", constraints);
            assertEquals(vertIDsList.size(), 1);
            assertEquals(addrRangeMap, conn.getVertByID(vertIDsList.get(0)));

            conn.removeAllVertices();
        } catch (RuntimeException e) {
            e.printStackTrace();
        } 
    }

    public void testLoad() {
        String addrRangeString =
        "{" +
        "      \"endIP\": \"216.98.188.255\"," +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" id=\\\"stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe\\\"><cybox:Title>AddressRange<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">CAIDA<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:addressRange-3630349312-3630349567\\\"><cybox:Description>216.98.188.0 through 216.98.188.255<\\/cybox:Description><cybox:Properties xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\" apply_condition=\\\"ANY\\\" condition=\\\"InclusiveBetween\\\" delimiter=\\\" - \\\">216.98.188.0 - 216.98.188.255<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\","+
        "      \"vertexType\": \"AddressRange\"," +
        "      \"startIP\": \"216.98.188.0\"," +
        "      \"startIPInt\": 3630349312," +
        "      \"name\": \"216.98.188.0 - 216.98.188.255\"," +
        "      \"description\": [\"216.98.188.0 through 216.98.188.255\"]," +
        "      \"source\": [\"CAIDA\"]," +
        "      \"endIPInt\": 3630349567," +
        "      \"observableType\": \"Address\"" +
        "}";

        String ipString =
        "{" +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" id=\\\"stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241\\\"><cybox:Title>IP<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">LoginEvent<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:ip-3232238091\\\"><cybox:Description>192.168.10.11<\\/cybox:Description><cybox:Properties xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\">216.98.188.1<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\","+
        "      \"vertexType\": \"IP\"," +
        "      \"ipInt\": 3630349313," +
        "      \"name\": \"216.98.188.1\"," +
        "      \"description\": [\"216.98.188.1\", \"Some other description .\"]," +
        "      \"source\": [\"LoginEvent\", \"maxmind\"]," +
        "      \"observableType\": \"Address\"" + 
        "}";

        try {
            Map<String, Object> addrRangeMap = conn.jsonVertToMap(new JSONObject(addrRangeString));
            String addrRangeID = conn.addVertex(addrRangeMap);
            assertEquals(addrRangeMap, conn.getVertByID(addrRangeID));



            Map<String, Object> ipMap = conn.jsonVertToMap(new JSONObject(ipString));
            String ipID = conn.addVertex(ipMap);
            assertEquals(ipMap, conn.getVertByID(ipID));
        } catch (RuntimeException e) {
            e.printStackTrace();
        } 
    }
}


