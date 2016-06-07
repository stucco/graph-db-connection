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
import java.lang.Number;
 
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
        conn.removeAllVertices();
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

    public void testComprehensive() {
        String flowString =
        "{ " +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:flow-da6b7a73-6ed4-4d9a-b8dd-b770e2619ffb\\\"><cybox:Title>Flow<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:flow-168430081_56867-168430180_22\\\"><cybox:Description>10.10.10.1, port 56867 to 10.10.10.100, port 22<\\/cybox:Description><cybox:Properties xmlns:NetFlowObj=\\\"http://cybox.mitre.org/objects#NetworkFlowObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"NetFlowObj:NetworkFlowObjectType\\\"><cyboxCommon:Custom_Properties xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\"><cyboxCommon:Property name=\\\"TotBytes\\\">585<\\/cyboxCommon:Property><cyboxCommon:Property name=\\\"Flgs\\\"> e s<\\/cyboxCommon:Property><cyboxCommon:Property name=\\\"State\\\">REQ<\\/cyboxCommon:Property><cyboxCommon:Property name=\\\"StartTime\\\">1373553586.136399<\\/cyboxCommon:Property><cyboxCommon:Property name=\\\"Dir\\\">-&gt;<\\/cyboxCommon:Property><cyboxCommon:Property name=\\\"TotPkts\\\">8<\\/cyboxCommon:Property><\\/cyboxCommon:Custom_Properties><NetFlowObj:Network_Flow_Label><NetFlowObj:Src_Socket_Address object_reference=\\\"stucco:address-f6e40756-f29f-462c-aa9d-3c90af97626f\\\" /><NetFlowObj:Dest_Socket_Address object_reference=\\\"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\\\" /><NetFlowObj:IP_Protocol>6<\\/NetFlowObj:IP_Protocol><\\/NetFlowObj:Network_Flow_Label><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"Observable\", " +
        "      \"name\": \"10.10.10.1:56867_through_10.10.10.100:22\", " +
        "      \"description\": [\"10.10.10.1, port 56867 to 10.10.10.100, port 22\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Network Flow\" " +
        "}";

        String srcAddressString = 
        "{" +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:address-f6e40756-f29f-462c-aa9d-3c90af97626f\\\"><cybox:Title>Address<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:address-168430081_56867\\\"><cybox:Description>10.10.10.1, port 56867<\\/cybox:Description><cybox:Properties xmlns:SocketAddressObj=\\\"http://cybox.mitre.org/objects#SocketAddressObject-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"SocketAddressObj:SocketAddressObjectType\\\"><SocketAddressObj:IP_Address object_reference=\\\"stucco:ip-8134dbc0-ffa4-44cd-89d2-1d7428c08489\\\" /><SocketAddressObj:Port object_reference=\\\"stucco:port-6e8e3e78-962a-408e-9495-be65b11fff09\\\" /><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"Observable\", " +
        "      \"name\": \"10.10.10.1:56867\", " +
        "      \"description\": [\"10.10.10.1, port 56867\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Socket Address\" " +
        "}";

        String srcIPString = 
        "{" +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:ip-8134dbc0-ffa4-44cd-89d2-1d7428c08489\\\"><cybox:Title>IP<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:ip-168430081\\\"><cybox:Description>10.10.10.1<\\/cybox:Description><cybox:Properties xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value>10.10.10.1<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"IP\", " +
        "      \"ipInt\": 168430081, " +
        "      \"name\": \"10.10.10.1\", " +
        "      \"description\": [\"10.10.10.1\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Address\" " +
        "}";

        String srcPortString =
        "{" +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:port-6e8e3e78-962a-408e-9495-be65b11fff09\\\"><cybox:Title>Port<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:port-56867\\\"><cybox:Description>56867<\\/cybox:Description><cybox:Properties xmlns:PortObj=\\\"http://cybox.mitre.org/objects#PortObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"PortObj:PortObjectType\\\"><PortObj:Port_Value>56867<\\/PortObj:Port_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"Observable\", " +
        "      \"name\": \"56867\", " +
        "      \"description\": [\"56867\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Port\" " +
        "}";

        String destAddressString = 
        "{" +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\\\"><cybox:Title>Address<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:address-168430180_22\\\"><cybox:Description>10.10.10.100, port 22<\\/cybox:Description><cybox:Properties xmlns:SocketAddressObj=\\\"http://cybox.mitre.org/objects#SocketAddressObject-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"SocketAddressObj:SocketAddressObjectType\\\"><SocketAddressObj:IP_Address object_reference=\\\"stucco:ip-a5dff0b3-0f2f-4308-a16d-949c5826cf1a\\\" /><SocketAddressObj:Port object_reference=\\\"stucco:port-2ce88ec7-6ace-4d70-aa31-ad6aa8129f26\\\" /><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"Observable\", " +
        "      \"name\": \"10.10.10.100:22\", " +
        "      \"description\": [\"10.10.10.100, port 22\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Socket Address\" " +
        "}";

        String destIPString = 
        "{" +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:ip-a5dff0b3-0f2f-4308-a16d-949c5826cf1a\\\"><cybox:Title>IP<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:ip-168430180\\\"><cybox:Description>10.10.10.100<\\/cybox:Description><cybox:Properties xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value>10.10.10.100<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"IP\", " +
        "      \"ipInt\": 168430180, " +
        "      \"name\": \"10.10.10.100\", " +
        "      \"description\": [\"10.10.10.100\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Address\" " +
        "}";

        String destPortString = 
        "{ " +
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:port-2ce88ec7-6ace-4d70-aa31-ad6aa8129f26\\\"><cybox:Title>Port<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:port-22\\\"><cybox:Description>22<\\/cybox:Description><cybox:Properties xmlns:PortObj=\\\"http://cybox.mitre.org/objects#PortObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"PortObj:PortObjectType\\\"><PortObj:Port_Value>22<\\/PortObj:Port_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"Observable\", " +
        "      \"name\": \"22\", " +
        "      \"description\": [\"22\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Port\" " +
        "}";

        try {
            //add/get flow vert 
            Map<String, Object> flowMap = conn.jsonVertToMap(new JSONObject(flowString));
            final String flowID = conn.addVertex(flowMap);
            assertEquals(flowMap, conn.getVertByID(flowID));
            
            //add/get src address vert
            Map<String, Object> srcAddressMap = conn.jsonVertToMap(new JSONObject(srcAddressString));
            final String srcAddressID = conn.addVertex(srcAddressMap);
            assertEquals(srcAddressMap, conn.getVertByID(srcAddressID));

            //add/get src ip vert
            Map<String, Object> srcIPMap = conn.jsonVertToMap(new JSONObject(srcIPString));
            final String srcIPID = conn.addVertex(srcIPMap);            assertEquals(srcIPMap, conn.getVertByID(srcIPID));

            //add/get src port vert
            Map<String, Object> srcPortMap = conn.jsonVertToMap(new JSONObject(srcPortString));
            final String srcPortID = conn.addVertex(srcPortMap);
            assertEquals(srcPortMap, conn.getVertByID(srcPortID));

            //add/get dest address vert
            Map<String, Object> destAddressMap = conn.jsonVertToMap(new JSONObject(destAddressString));
            final String destAddressID = conn.addVertex(destAddressMap);
            assertEquals(destAddressMap, conn.getVertByID(destAddressID));

            //add/get dest ip vert
            Map<String, Object> destIPMap = conn.jsonVertToMap(new JSONObject(destIPString));
            final String destIPID = conn.addVertex(destIPMap);
            assertEquals(destIPMap, conn.getVertByID(destIPID));

            //add/get dest port vert
            Map<String, Object> destPortMap = conn.jsonVertToMap(new JSONObject(destPortString));
            final String destPortID = conn.addVertex(destPortMap);
            assertEquals(destPortMap, conn.getVertByID(destPortID));
            
            //add flow -> src address edge
            conn.addEdge(srcAddressID, flowID, "Sub-Observable");

            //add flow -> dest address edge
            conn.addEdge(destAddressID, flowID, "Sub-Observable");

            //add srcAddress -> srcIP address edge
            conn.addEdge(srcIPID, srcAddressID, "Sub-Observable");

            //add srcAddress -> srcPort address edge
            conn.addEdge(srcPortID, srcAddressID, "Sub-Observable");

            //add destAddress -> destIP address edge
            conn.addEdge(destIPID, destAddressID, "Sub-Observable");

            //add destAddress -> destPort address edge
            conn.addEdge(destPortID, destAddressID, "Sub-Observable");

            //getOutEdges
            List<Map<String, Object>> flowOutEdges = conn.getOutEdges(flowID);
            assertTrue(flowOutEdges.contains(new HashMap<String, Object>(){{put("outVertID", flowID); put("inVertID", srcAddressID); put("relation", "Sub-Observable");}}));
            assertTrue(flowOutEdges.contains(new HashMap<String, Object>(){{put("outVertID", flowID); put("inVertID", destAddressID); put("relation", "Sub-Observable");}}));

            List<Map<String, Object>> srcAddressOutEdges = conn.getOutEdges(srcAddressID);
            assertTrue(srcAddressOutEdges.contains(new HashMap<String, Object>(){{put("outVertID", srcAddressID); put("inVertID", srcIPID); put("relation", "Sub-Observable");}}));
            assertTrue(srcAddressOutEdges.contains(new HashMap<String, Object>(){{put("outVertID", srcAddressID); put("inVertID", srcPortID); put("relation", "Sub-Observable");}}));

            List<Map<String, Object>> destAddressOutEdges = conn.getOutEdges(destAddressID);
            assertTrue(destAddressOutEdges.contains(new HashMap<String, Object>(){{put("outVertID", destAddressID); put("inVertID", destIPID); put("relation", "Sub-Observable");}}));
            assertTrue(destAddressOutEdges.contains(new HashMap<String, Object>(){{put("outVertID", destAddressID); put("inVertID", destPortID); put("relation", "Sub-Observable");}}));

            //getInEdges
            List<Map<String, Object>> srcAddressInEdges = conn.getInEdges(srcAddressID);
            assertTrue(srcAddressInEdges.size() == 1);
            assertTrue(srcAddressInEdges.contains(new HashMap<String, Object>(){{put("outVertID", flowID); put("inVertID", srcAddressID); put("relation", "Sub-Observable");}}));

            List<Map<String, Object>> destAddressInEdges = conn.getInEdges(destAddressID);
            assertTrue(destAddressInEdges.size() == 1);
            assertTrue(destAddressInEdges.contains(new HashMap<String, Object>(){{put("outVertID", flowID); put("inVertID", destAddressID); put("relation", "Sub-Observable");}}));

            List<Map<String, Object>> srcIPInEdges = conn.getInEdges(srcIPID);
            assertTrue(srcIPInEdges.size() == 1);
            assertTrue(srcIPInEdges.contains(new HashMap<String, Object>(){{put("outVertID", srcAddressID); put("inVertID", srcIPID); put("relation", "Sub-Observable");}}));

            List<Map<String, Object>> destIPInEdges = conn.getInEdges(destIPID);
            assertTrue(destIPInEdges.size() == 1);
            assertTrue(destIPInEdges.contains(new HashMap<String, Object>(){{put("outVertID", destAddressID); put("inVertID", destIPID); put("relation", "Sub-Observable");}})); 

            List<Map<String, Object>> srcPortInEdges = conn.getInEdges(srcPortID);
            assertTrue(srcPortInEdges.size() == 1);
            assertTrue(srcPortInEdges.contains(new HashMap<String, Object>(){{put("outVertID", srcAddressID); put("inVertID", srcPortID); put("relation", "Sub-Observable");}}));

            List<Map<String, Object>> destPortInEdges = conn.getInEdges(destPortID);
            assertTrue(destPortInEdges.size() == 1);
            assertTrue(destPortInEdges.contains(new HashMap<String, Object>(){{put("outVertID", destAddressID); put("inVertID", destPortID); put("relation", "Sub-Observable");}}));

            //List<String> getInVertIDsByRelation(String outVertID, String relation)
            List<String> flowInVertIDs = conn.getInVertIDsByRelation(flowID, "Sub-Observable");
            assertTrue(flowInVertIDs.size() == 2);
            assertTrue(flowInVertIDs.contains(srcAddressID));
            assertTrue(flowInVertIDs.contains(destAddressID));

            List<String> srcAddressInVertIDs = conn.getInVertIDsByRelation(srcAddressID, "Sub-Observable");
            assertTrue(srcAddressInVertIDs.size() == 2);
            assertTrue(srcAddressInVertIDs.contains(srcIPID));
            assertTrue(srcAddressInVertIDs.contains(srcPortID));

            List<String> destAddressInVertIDs = conn.getInVertIDsByRelation(destAddressID, "Sub-Observable");
            assertTrue(destAddressInVertIDs.size() == 2);
            assertTrue(destAddressInVertIDs.contains(destIPID));
            assertTrue(destAddressInVertIDs.contains(destPortID));

            List<String> emptyInIDs = conn.getInVertIDsByRelation(srcIPID, "Sub-Observable");
            emptyInIDs.addAll(conn.getInVertIDsByRelation(srcPortID, "Sub-Observable"));
            emptyInIDs.addAll(conn.getInVertIDsByRelation(destIPID, "Sub-Observable"));
            emptyInIDs.addAll(conn.getInVertIDsByRelation(destPortID, "Sub-Observable"));
            assertTrue(emptyInIDs.isEmpty());

            //List<String> getInVertIDsByRelation(String outVertID, String relation, List<DBConstraint> constraints)
            srcAddressInVertIDs = conn.getInVertIDsByRelation(flowID, "With-Observable", new ArrayList<DBConstraint>());
            assertTrue(srcAddressInVertIDs.isEmpty());

            List<String> inVertIDs = conn.getInVertIDsByRelation(flowID, "Sub-Observable", Arrays.asList(conn.getConstraint("name", Condition.eq, "10.10.10.1:56867")));
            assertTrue(inVertIDs.size() == 1);
            assertTrue(inVertIDs.contains(srcAddressID));

            //List<String> getOutVertIDsByRelation(String inVertID, String relation) {
            List<String> outVertIDs = conn.getOutVertIDsByRelation(srcIPID, "Sub-Observable");
            outVertIDs.addAll(conn.getOutVertIDsByRelation(srcPortID, "Sub-Observable"));
            assertTrue(outVertIDs.size() == 2);
            assertEquals(outVertIDs.get(0), outVertIDs.get(1));

            outVertIDs = conn.getOutVertIDsByRelation(destIPID, "Sub-Observable");
            outVertIDs.addAll(conn.getOutVertIDsByRelation(destPortID, "Sub-Observable"));
            assertTrue(outVertIDs.size() == 2);
            assertEquals(outVertIDs.get(0), outVertIDs.get(1));

            outVertIDs = conn.getOutVertIDsByRelation(srcAddressID, "Sub-Observable");
            outVertIDs.addAll(conn.getOutVertIDsByRelation(destAddressID, "Sub-Observable"));
            assertTrue(outVertIDs.size() == 2);
            assertEquals(outVertIDs.get(0), outVertIDs.get(1));
            
            //List<String> getOutVertIDsByRelation(String inVertID, String relation, List<DBConstraint> constraints)
            outVertIDs = conn.getOutVertIDsByRelation(srcAddressID, "Sub-Observable", Arrays.asList(conn.getConstraint("observableType", Condition.eq, "Network Flow")));
            outVertIDs.addAll(conn.getOutVertIDsByRelation(destAddressID, "Sub-Observable", Arrays.asList(conn.getConstraint("observableType", Condition.eq, "Network Flow"))));
            assertTrue(outVertIDs.size() == 2);
            assertEquals(outVertIDs.get(0), outVertIDs.get(1));

            //public List<String> getVertIDsByRelation(String vertID, String relation)
            List<String> vertIDs = conn.getVertIDsByRelation(flowID, "Sub-Observable");
            assertTrue(vertIDs.contains(srcAddressID));
            assertTrue(vertIDs.contains(destAddressID));

            vertIDs = conn.getVertIDsByRelation(srcAddressID, "Sub-Observable");
            assertTrue(vertIDs.size() == 3);
            assertTrue(vertIDs.contains(flowID));
            assertTrue(vertIDs.contains(srcIPID));
            assertTrue(vertIDs.contains(srcPortID));

            vertIDs = conn.getVertIDsByRelation(destAddressID, "Sub-Observable");
            assertTrue(vertIDs.size() == 3);
            assertTrue(vertIDs.contains(flowID));
            assertTrue(vertIDs.contains(destIPID));
            assertTrue(vertIDs.contains(destPortID));

            vertIDs = conn.getVertIDsByRelation("da6b7a73-6ed4-4d9a-b8dd-b770e2619ffb", "Sub-Observable");
            assertTrue(vertIDs.isEmpty());

            //public List<String> getVertIDsByRelation(String vertID, String relation, List<DBConstraint> constraints)
            vertIDs = conn.getVertIDsByRelation(srcAddressID, "Sub-Observable", Arrays.asList(conn.getConstraint("observableType", Condition.eq, "Network Flow")));
            assertTrue(vertIDs.size() == 1 && vertIDs.contains(flowID));

            vertIDs = conn.getVertIDsByRelation(destAddressID, "Sub-Observable", Arrays.asList(conn.getConstraint("source", Condition.contains, "Argus")));
            assertTrue(vertIDs.size() == 3 && vertIDs.contains(flowID) &&  vertIDs.contains(destIPID) &&  vertIDs.contains(destPortID));

            //public void updateVertex(String vertID, Map<String, Object> properties)
            flowMap.put("name", "some new name");
            flowMap.put("sourceDocument", "some new sourceDocument");
            conn.updateVertex(flowID, flowMap);
            Map<String, Object> newFlow = conn.getVertByID(flowID);
            assertEquals(newFlow.get("name"), "some new name");
            assertEquals(newFlow.get("sourceDocument"), "some new sourceDocument");

            //void removeVertByID(String vertID)
            conn.removeVertByID(srcAddressID);
            assertEquals(conn.getVertCount(), 6);
            assertEquals(conn.getEdgeCount(), 3);


            //void removeEdgeByRelation(String inVertID, String outVertID, String relation)
            conn.removeEdgeByRelation(destAddressID, flowID, "Sub-Observable");
            conn.removeEdgeByRelation(destIPID, destAddressID, "Sub-Observable");
            assertEquals(conn.getEdgeCount(), 1);

            //void removeAllVertices()
            conn.removeAllVertices();
            assertEquals(conn.getVertCount(), 0);
            assertEquals(conn.getEdgeCount(), 0);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }
}


