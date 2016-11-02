package gov.pnnl.stucco.dbconnect.postgresql;

import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionTestInterface;
import gov.pnnl.stucco.dbconnect.DBConstraint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map; 
import java.util.Set;

import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
 
import org.junit.After; 
import org.junit.Before; 
import org.junit.Test;  
 
import org.json.JSONObject;
import org.json.JSONArray; 

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.csv.CSVRecord; 
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

/**
 * Unit test for PostgresDBConnection
 */ 
public class PostgresqlDBConnectionTest {
    private static DBConnectionFactory factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.POSTGRESQL);
    private static DBConnectionTestInterface conn;

    private Map<String, Object> convertEdgeToMap (JSONObject edge) {
        Map<String, Object> edgeMap = new HashMap<String, Object>();
        for (String key : (Set<String>) edge.keySet()) {
            edgeMap.put(key, edge.get(key));
        }

        return edgeMap;
    }

    @Before
    public void init() {
        factory.setConfiguration("./config/postgresql.yml");
        conn = factory.getDBConnectionTestInterface();
        conn.open();
    }

    @After
    public void close() {
        conn.close();
    } 

    @Test
    public void testLoadGraph () {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testLoadGraph()");
        
        String graphString =
          "{"+
          "  \"vertices\": {"+
          "    \"Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402\": {"+
          "      \"vertexType\": \"Indicator\","+
          "      \"sourceDocument\": \"<stix:Indicator xmlns:stix=\\\"http://stix.mitre.org/stix-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"indicator:IndicatorType\\\" id=\\\"Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402\\\"><indicator:Title xmlns:indicator=\\\"http://stix.mitre.org/Indicator-2\\\">Indicator One Title<\\/indicator:Title><indicator:Indicated_TTP xmlns:indicator=\\\"http://stix.mitre.org/Indicator-2\\\"><stixCommon:TTP xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xsi:type=\\\"ttp:TTPType\\\" idref=\\\"TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70\\\" /><\\/indicator:Indicated_TTP><\\/stix:Indicator>\","+
          "      \"name\": \"Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402\""+
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
          "      \"outVertTable\": \"Indicator\","+
                "      \"inVertTable\": \"TTP\","+
          "      \"relation\": \"IndicatedTTP\""+
          "    }"+
          "  ]"+
          "}";

        String outVertID = "Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402";
        String inVertID = "TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70";

        JSONObject graph = new JSONObject(graphString);
        conn.bulkLoadGraph(graph);

        JSONObject vertices = graph.getJSONObject("vertices");
          
        Map<String, Object> indicatorVertex = conn.jsonVertToMap(vertices.getJSONObject("Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402")); 
        Map<String, Object> expectedIndicatorVertex = conn.getVertByID("Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402");
        assertEquals(indicatorVertex, expectedIndicatorVertex);

        Map<String, Object> ttpVertex = conn.jsonVertToMap(vertices.getJSONObject("TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70"));
        Map<String, Object> expectedTTPVertex = conn.getVertByID("TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70");
        assertEquals(ttpVertex, expectedTTPVertex);

        Map<String, Object> edge = new HashMap<String, Object>();
        edge.put("outVertID", "Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402");
        edge.put("inVertID", "TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70");
        edge.put("outVertTable", "Indicator");
        edge.put("inVertTable", "TTP");
        edge.put("relation", "IndicatedTTP");

        List<Map<String, Object>> outEdges = conn.getOutEdges(outVertID);
        assertTrue(outEdges.contains(edge));

        List<Map<String, Object>> inEdges = conn.getInEdges(inVertID);
        assertTrue(outEdges.contains(edge));

        for (String id : (Set<String>) graph.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
    }

    @Test
    public void testUpdateProperties() {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testUpdateProperties()");

        String vertString =
                "{"+
            "  \"vertices\": {"+
            "    \"stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe\": {"+
            "      \"endIP\": \"216.98.188.255\","+
            "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" id=\\\"stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe\\\"><cybox:Title>AddressRange<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">CAIDA<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:addressRange-3630349312-3630349567\\\"><cybox:Description>216.98.188.0 through 216.98.188.255<\\/cybox:Description><cybox:Properties xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\" apply_condition=\\\"ANY\\\" condition=\\\"InclusiveBetween\\\" delimiter=\\\" - \\\">216.98.188.0 - 216.98.188.255<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\","+
            "      \"vertexType\": \"AddressRange\","+
            "      \"startIP\": \"216.98.188.0\","+
            "      \"startIPInt\": 3630349312,"+
            "      \"name\": \"216.98.188.0 - 216.98.188.255\","+
            "      \"source\": [\"CAIDA\"],"+
            "      \"endIPInt\": 3630349567,"+
            "      \"observableType\": \"Address\""+
            "}}}";

        String vertUpdatedString = 
            "{"+
            "  \"vertices\": {"+
            "    \"stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe\": {"+
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
            "}}}";

        JSONObject graph1 = new JSONObject(vertString);
        JSONObject graph2 = new JSONObject(vertUpdatedString);

        conn.bulkLoadGraph(graph1);

        JSONObject json = new JSONObject(vertString).getJSONObject("vertices").getJSONObject("stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe");
        Map<String, Object> vert = conn.jsonVertToMap(json);
        Map<String, Object> dbVert = conn.getVertByID("stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe");
        assertEquals(dbVert, vert);

        Map<String, Object> vertUpdate = conn.jsonVertToMap(graph2.getJSONObject("vertices").getJSONObject("stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe"));
        conn.updateVertex("stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe", vertUpdate);
        Map<String, Object> dbVertUpdate = conn.getVertByID("stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe");
        assertEquals(vertUpdate, dbVertUpdate);

        for (String id : (Set<String>) graph1.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
        for (String id : (Set<String>) graph2.getJSONObject("vertices").keySet()) {
        conn.removeVertByID(id);
        }
    }

    @Test
    public void testGetVertIDsByRelation() {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testGetVertIDsByRelation()");

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
        "      \"name\": \"Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb\""+
        "    }"+
        "  },"+
        "  \"edges\": ["+
        "    {"+
        "      \"outVertID\": \"Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb\","+
        "      \"inVertID\": \"TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341\","+
        "      \"outVertTable\": \"Indicator\","+
        "      \"inVertTable\": \"TTP\","+
        "      \"relation\": \"IndicatedTTP\""+
        "    },"+
        "    {"+
        "      \"outVertID\": \"Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb\","+
        "      \"inVertID\": \"Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb\","+
        "      \"outVertTable\": \"Indicator\","+
        "      \"inVertTable\": \"Course_Of_Action\","+
        "      \"relation\": \"SuggestedCOA\""+
        "    }"+
        "  ]"+
        "}";

        JSONObject graph = new JSONObject(graphString);
        JSONObject verts = graph.getJSONObject("vertices");
        JSONArray edges = graph.getJSONArray("edges");

        conn.bulkLoadGraph(graph);

        Map<String, Object> indicatorMap = conn.jsonVertToMap(verts.getJSONObject("Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb"));
        Map<String, Object> indicatorMapDB = conn.getVertByID("Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb");
        assertEquals(indicatorMap, indicatorMapDB);

        Map<String, Object> ttpMap = conn.jsonVertToMap(verts.getJSONObject("TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341"));
        Map<String, Object> ttpMapDB = conn.getVertByID("TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341");
        assertEquals(ttpMap, ttpMapDB);

        Map<String, Object> coaMap = conn.jsonVertToMap(verts.getJSONObject("Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb"));
        Map<String, Object> coaMapDB = conn.getVertByID("Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb");
        assertEquals(coaMap, coaMapDB);

        List<String> inVertIDs = conn.getInVertIDsByRelation("Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb", "IndicatedTTP");
        assertTrue(inVertIDs.contains("TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341"));

        inVertIDs = conn.getInVertIDsByRelation("Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb", "SuggestedCOA");
        assertTrue(inVertIDs.contains("Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb"));

        List<String> vertIDs = conn.getVertIDsByRelation("Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb", "IndicatedTTP");
        assertTrue(vertIDs.contains("TTP-c7561b63-ab62-433e-a5c2-b330c1dcc341"));

        vertIDs = conn.getVertIDsByRelation("Indicator-c304f71f-788d-46cb-919d-da1ca4c781bb", "SuggestedCOA");
        assertTrue(vertIDs.contains("Course_Of_Action-ae6c9867-9433-481c-80e5-4672d92811bb"));

        for (String id : (Set<String>) graph.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
    }

    @Test
    public void testConstraints() {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testConstraints()");

        String addrRangeString =
        "{" +
        "  \"vertices\": {"+
        "    \"stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe\": {"+
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
        "}}}";

        String ipString =
        "{" +
        "  \"vertices\": {"+
        "    \"stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241\": {"+
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" id=\\\"stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241\\\"><cybox:Title>IP<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">LoginEvent<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:ip-3232238091\\\"><cybox:Description>192.168.10.11<\\/cybox:Description><cybox:Properties xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\">216.98.188.1<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\","+
        "      \"vertexType\": \"IP\"," +
        "      \"ipInt\": 3630349313," +
        "      \"name\": \"216.98.188.1\"," +
        "      \"description\": [\"216.98.188.1\", \"Some other description.\"]," +
        "      \"source\": [\"LoginEvent\", \"maxmind\"]," +
        "      \"observableType\": \"Address\"" + 
        "}}}";

        JSONObject graph1 = new JSONObject(addrRangeString);
        JSONObject graph2 = new JSONObject(ipString);

        conn.bulkLoadGraph(graph1);
        conn.bulkLoadGraph(graph2);

        try {
            String addrRangeID = "stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe";
            String ipID = "stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241";
            JSONObject ar = new JSONObject(addrRangeString).getJSONObject("vertices").getJSONObject(addrRangeID);
            JSONObject ip = new JSONObject(ipString).getJSONObject("vertices").getJSONObject(ipID);

            Map<String, Object> addrRangeMap = conn.jsonVertToMap(ar);
            Map<String, Object> addrRangeMapDB = conn.getVertByID(addrRangeID);
            assertEquals(addrRangeMap, addrRangeMapDB);

            Map<String, Object> ipMap = conn.jsonVertToMap(ip);
            Map<String, Object> ipMapDB = conn.getVertByID(ipID);
            assertEquals(ipMap, ipMapDB);

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
        } catch (RuntimeException e) {
            e.printStackTrace();
        } 

        for (String id : (Set<String>) graph1.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
        for (String id : (Set<String>) graph2.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
    }

    @Test
    public void testConstraintsTwo() {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testConstraintsTwo()");

        String addrRangeString =
        "{" +
        "  \"vertices\": {"+
        "    \"stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe\": {"+
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
        "}}}";

        String ipString =
        "{" +
        "  \"vertices\": {"+
        "    \"stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241\": {"+
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" id=\\\"stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241\\\"><cybox:Title>IP<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">LoginEvent<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:ip-3232238091\\\"><cybox:Description>192.168.10.11<\\/cybox:Description><cybox:Properties xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\">216.98.188.1<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\","+
        "      \"vertexType\": \"IP\"," +
        "      \"ipInt\": 3630349313," +
        "      \"name\": \"216.98.188.1\"," +
        "      \"description\": [\"216.98.188.1\", \"Some other description.\"]," +
        "      \"source\": [\"LoginEvent\", \"maxmind\"]," +
        "      \"observableType\": \"Address\"" + 
        "}}}";

        JSONObject graph1 = new JSONObject(ipString);
        JSONObject graph2 = new JSONObject(addrRangeString);

        Map<String, Object> edge = new HashMap<String, Object>();
        edge.put("outVertID", "stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241");
        edge.put("inVertID", "stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe");
        edge.put("outVertTable", "IP");
        edge.put("inVertTable", "AddressRange");
        edge.put("relation", "Contained_Within");

        conn.bulkLoadGraph(graph1);
        conn.bulkLoadGraph(graph2);

        try {
            String addrRangeID = "stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe";
            String ipID = "stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241";

            JSONObject ar = new JSONObject(addrRangeString).getJSONObject("vertices").getJSONObject(addrRangeID);
            JSONObject ip = new JSONObject(ipString).getJSONObject("vertices").getJSONObject(ipID);

          Map<String, Object> addrRangeMap = conn.jsonVertToMap(ar);
          Map<String, Object> addrRangeMapDB = conn.getVertByID(addrRangeID);
          assertEquals(addrRangeMap, addrRangeMapDB);

          Map<String, Object> ipMap = conn.jsonVertToMap(ip);
          Map<String, Object> ipMapDB = conn.getVertByID(ipID);
          assertEquals(ipMap, ipMapDB);

          List<Map<String,Object>> inEdges = conn.getInEdges(addrRangeID);
          System.out.println(inEdges);
          assertTrue(inEdges.contains(edge));

          List<Map<String,Object>> outEdges = conn.getOutEdges(ipID);
          assertTrue(inEdges.contains(edge));

          List<DBConstraint> constraints = new ArrayList<DBConstraint>();
          constraints.add(conn.getConstraint("vertexType", Condition.eq, "IP"));
          constraints.add(conn.getConstraint("ipInt", Condition.eq, 3630349313L));
          List<String> vertIDs = conn.getVertIDsByConstraints(constraints);
          assertTrue(vertIDs.contains("stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241"));         

          constraints = new ArrayList<DBConstraint>();
          constraints.add(conn.getConstraint("startIPInt", Condition.gte, 3630349312L));
          constraints.add(conn.getConstraint("endIPInt", Condition.lte, 3630349567L));
          vertIDs = conn.getVertIDsByConstraints(constraints);
          assertTrue(vertIDs.contains("stucco:addressRange-33f72b4c-e6f2-4d82-88d4-2a7711ce7bfe"));

          constraints = new ArrayList<DBConstraint>();
          constraints.add(conn.getConstraint("source", Condition.contains, "maxmind"));
          List<String> vertIDsList = conn.getVertIDsByConstraints(constraints);
          assertTrue(vertIDsList.contains(ipID));

          constraints = new ArrayList<DBConstraint>();
          constraints.add(conn.getConstraint("description", Condition.contains, "Some other description."));
          vertIDsList = conn.getVertIDsByConstraints(constraints);
          assertTrue(vertIDsList.contains(ipID));

          constraints = new ArrayList<DBConstraint>();
          constraints.add(conn.getConstraint("sourceDocument", Condition.substring, "216.98.188.0 - 216.98.188.255"));
          vertIDsList = conn.getVertIDsByConstraints(constraints);
          assertTrue(vertIDsList.contains(addrRangeID));

          constraints = new ArrayList<DBConstraint>();
          constraints.add(conn.getConstraint("sourceDocument", Condition.substring, "CAIDA"));
          vertIDsList = conn.getVertIDsByRelation(ipID, "Contained_Within", constraints);
          assertTrue(vertIDsList.contains(addrRangeID));

          constraints = new ArrayList<DBConstraint>();
          constraints.add(conn.getConstraint("sourceDocument", Condition.substring, "LoginEvent"));
          vertIDsList = conn.getOutVertIDsByRelation(addrRangeID, "Contained_Within", constraints);
          assertTrue(vertIDsList.contains(ipID));

          constraints = new ArrayList<DBConstraint>();
          constraints.add(conn.getConstraint("sourceDocument", Condition.substring, "CAIDA"));
          vertIDsList = conn.getInVertIDsByRelation(ipID, "Contained_Within", constraints);
          assertTrue(vertIDsList.contains(addrRangeID));
        } catch (RuntimeException e) {
            e.printStackTrace();
        } 
        for (String id : (Set<String>) graph1.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
        for (String id : (Set<String>) graph2.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
    }

    @Test
    public void testComprehensive() {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testComprehensive()");

        String flowString =
        "{" +
        "  \"vertices\": {"+
        "    \"stucco:flow-da6b7a73-6ed4-4d9a-b8dd-b770e2619ffb\": {"+
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:flow-da6b7a73-6ed4-4d9a-b8dd-b770e2619ffb\\\"><cybox:Title>Flow<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:flow-168430081_56867-168430180_22\\\"><cybox:Description>10.10.10.1, port 56867 to 10.10.10.100, port 22<\\/cybox:Description><cybox:Properties xmlns:NetFlowObj=\\\"http://cybox.mitre.org/objects#NetworkFlowObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"NetFlowObj:NetworkFlowObjectType\\\"><cyboxCommon:Custom_Properties xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\"><cyboxCommon:Property name=\\\"TotBytes\\\">585<\\/cyboxCommon:Property><cyboxCommon:Property name=\\\"Flgs\\\"> e s<\\/cyboxCommon:Property><cyboxCommon:Property name=\\\"State\\\">REQ<\\/cyboxCommon:Property><cyboxCommon:Property name=\\\"StartTime\\\">1373553586.136399<\\/cyboxCommon:Property><cyboxCommon:Property name=\\\"Dir\\\">-&gt;<\\/cyboxCommon:Property><cyboxCommon:Property name=\\\"TotPkts\\\">8<\\/cyboxCommon:Property><\\/cyboxCommon:Custom_Properties><NetFlowObj:Network_Flow_Label><NetFlowObj:Src_Socket_Address object_reference=\\\"stucco:address-f6e40756-f29f-462c-aa9d-3c90af97626f\\\" /><NetFlowObj:Dest_Socket_Address object_reference=\\\"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\\\" /><NetFlowObj:IP_Protocol>6<\\/NetFlowObj:IP_Protocol><\\/NetFlowObj:Network_Flow_Label><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"Observable\", " +
        "      \"name\": \"10.10.10.1:56867_through_10.10.10.100:22\", " +
        "      \"description\": [\"10.10.10.1, port 56867 to 10.10.10.100, port 22\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Network Flow\" " +
        "           }," +
        "    \"stucco:address-f6e40756-f29f-462c-aa9d-3c90af97626f\": {"+
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:address-f6e40756-f29f-462c-aa9d-3c90af97626f\\\"><cybox:Title>Address<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:address-168430081_56867\\\"><cybox:Description>10.10.10.1, port 56867<\\/cybox:Description><cybox:Properties xmlns:SocketAddressObj=\\\"http://cybox.mitre.org/objects#SocketAddressObject-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"SocketAddressObj:SocketAddressObjectType\\\"><SocketAddressObj:IP_Address object_reference=\\\"stucco:ip-8134dbc0-ffa4-44cd-89d2-1d7428c08489\\\" /><SocketAddressObj:Port object_reference=\\\"stucco:port-6e8e3e78-962a-408e-9495-be65b11fff09\\\" /><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"Observable\", " +
        "      \"name\": \"10.10.10.1:56867\", " +
        "      \"description\": [\"10.10.10.1, port 56867\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Socket Address\" " +
        "           }," +
        "    \"stucco:ip-8134dbc0-ffa4-44cd-89d2-1d7428c08489\": {"+
        "      \"vertexType\": \"IP\", " +    
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:ip-8134dbc0-ffa4-44cd-89d2-1d7428c08489\\\"><cybox:Title>IP<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:ip-168430081\\\"><cybox:Description>10.10.10.1<\\/cybox:Description><cybox:Properties xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value>10.10.10.1<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"ipInt\": 168430081, " +
        "      \"name\": \"10.10.10.1\", " +
        "      \"description\": [\"10.10.10.1\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Address\" " +
        "           }," +
        "    \"stucco:port-6e8e3e78-962a-408e-9495-be65b11fff09\": {"+
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:port-6e8e3e78-962a-408e-9495-be65b11fff09\\\"><cybox:Title>Port<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:port-56867\\\"><cybox:Description>56867<\\/cybox:Description><cybox:Properties xmlns:PortObj=\\\"http://cybox.mitre.org/objects#PortObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"PortObj:PortObjectType\\\"><PortObj:Port_Value>56867<\\/PortObj:Port_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"Observable\", " +
        "      \"name\": \"56867\", " +
        "      \"description\": [\"56867\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Port\" " +
        "           }," +
        "    \"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\": {"+
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\\\"><cybox:Title>Address<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:address-168430180_22\\\"><cybox:Description>10.10.10.100, port 22<\\/cybox:Description><cybox:Properties xmlns:SocketAddressObj=\\\"http://cybox.mitre.org/objects#SocketAddressObject-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"SocketAddressObj:SocketAddressObjectType\\\"><SocketAddressObj:IP_Address object_reference=\\\"stucco:ip-a5dff0b3-0f2f-4308-a16d-949c5826cf1a\\\" /><SocketAddressObj:Port object_reference=\\\"stucco:port-2ce88ec7-6ace-4d70-aa31-ad6aa8129f26\\\" /><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"Observable\", " +
        "      \"name\": \"10.10.10.100:22\", " +
        "      \"description\": [\"10.10.10.100, port 22\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Socket Address\" " +
        "           }," +
        "    \"stucco:ip-a5dff0b3-0f2f-4308-a16d-949c5826cf1a\": {"+
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:ip-a5dff0b3-0f2f-4308-a16d-949c5826cf1a\\\"><cybox:Title>IP<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:ip-168430180\\\"><cybox:Description>10.10.10.100<\\/cybox:Description><cybox:Properties xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value>10.10.10.100<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"IP\", " +
        "      \"ipInt\": 168430180, " +
        "      \"name\": \"10.10.10.100\", " +
        "      \"description\": [\"10.10.10.100\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Address\" " +
        "           }," +
        "    \"stucco:port-2ce88ec7-6ace-4d70-aa31-ad6aa8129f26\": {"+
        "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:port-2ce88ec7-6ace-4d70-aa31-ad6aa8129f26\\\"><cybox:Title>Port<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:port-22\\\"><cybox:Description>22<\\/cybox:Description><cybox:Properties xmlns:PortObj=\\\"http://cybox.mitre.org/objects#PortObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"PortObj:PortObjectType\\\"><PortObj:Port_Value>22<\\/PortObj:Port_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
        "      \"vertexType\": \"Observable\", " +
        "      \"name\": \"22\", " +
        "      \"description\": [\"22\"], " +
        "      \"source\": [\"Argus\"], " +
        "      \"observableType\": \"Port\" " +
        "   }}, " +
        "  \"edges\": ["+
        "    {"+
        "      \"outVertID\": \"stucco:flow-da6b7a73-6ed4-4d9a-b8dd-b770e2619ffb\","+
        "      \"inVertID\": \"stucco:address-f6e40756-f29f-462c-aa9d-3c90af97626f\","+
        "      \"outVertTable\": \"Observable\","+
        "      \"inVertTable\": \"Observable\","+
        "      \"relation\": \"Sub-Observable\""+
        "    },"+
        "    {"+
        "      \"outVertID\": \"stucco:flow-da6b7a73-6ed4-4d9a-b8dd-b770e2619ffb\","+
        "      \"inVertID\": \"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\","+
        "      \"outVertTable\": \"Observable\","+
        "      \"inVertTable\": \"Observable\","+
        "      \"relation\": \"Sub-Observable\""+
        "    },"+
        "    {"+
        "      \"outVertID\": \"stucco:address-f6e40756-f29f-462c-aa9d-3c90af97626f\","+
        "      \"inVertID\": \"stucco:ip-8134dbc0-ffa4-44cd-89d2-1d7428c08489\","+
        "      \"outVertTable\": \"Observable\","+
        "      \"inVertTable\": \"IP\","+
        "      \"relation\": \"Sub-Observable\""+
        "    },"+
        "    {"+
        "      \"outVertID\": \"stucco:address-f6e40756-f29f-462c-aa9d-3c90af97626f\","+
        "      \"inVertID\": \"stucco:port-6e8e3e78-962a-408e-9495-be65b11fff09\","+
        "      \"outVertTable\": \"Observable\","+
        "      \"inVertTable\": \"Observable\","+
        "      \"relation\": \"Sub-Observable\""+
        "    },"+
        "    {"+
        "      \"outVertID\": \"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\","+
        "      \"inVertID\": \"stucco:ip-a5dff0b3-0f2f-4308-a16d-949c5826cf1a\","+
        "      \"outVertTable\": \"Observable\","+
        "      \"inVertTable\": \"IP\","+
        "      \"relation\": \"Sub-Observable\""+
        "    },"+
        "    {"+
        "      \"outVertID\": \"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\","+
        "      \"inVertID\": \"stucco:port-2ce88ec7-6ace-4d70-aa31-ad6aa8129f26\","+
        "      \"outVertTable\": \"Observable\","+
        "      \"inVertTable\": \"Observable\","+
        "      \"relation\": \"Sub-Observable\""+
        "    }"+
        "  ]"+
        "}";

        JSONObject graph = new JSONObject(flowString);
        JSONObject vertices = graph.getJSONObject("vertices");
        JSONArray edges = graph.getJSONArray("edges");

        conn.bulkLoadGraph(graph);

        String flowID = "stucco:flow-da6b7a73-6ed4-4d9a-b8dd-b770e2619ffb";
        String srcAddressID = "stucco:address-f6e40756-f29f-462c-aa9d-3c90af97626f";
        String destAddressID = "stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd";
        String srcIPID = "stucco:ip-8134dbc0-ffa4-44cd-89d2-1d7428c08489";
        String srcPortID = "stucco:port-6e8e3e78-962a-408e-9495-be65b11fff09";
        String destIPID = "stucco:ip-a5dff0b3-0f2f-4308-a16d-949c5826cf1a";
        String destPortID = "stucco:port-2ce88ec7-6ace-4d70-aa31-ad6aa8129f26";

        try {
            //add/get flow vert 
            Map<String, Object> flowMap = conn.jsonVertToMap(vertices.getJSONObject(flowID));
            Map<String, Object> flowMapDB = conn.getVertByID(flowID);
            assertEquals(flowMap, flowMapDB);
            
            //add/get src address vert
            Map<String, Object> srcAddressMap = conn.jsonVertToMap(vertices.getJSONObject(srcAddressID));
            Map<String, Object> srcAddressMapDB = conn.getVertByID(srcAddressID);
            assertEquals(srcAddressMap, srcAddressMapDB);

            //add/get src ip vert
            Map<String, Object> srcIPMap = conn.jsonVertToMap(vertices.getJSONObject(srcIPID));
            Map<String, Object> srcIPMapDB = conn.getVertByID(srcIPID);    
            Number ipInt1 = (Number) srcIPMap.get("ipInt");
            srcIPMap.put("ipInt", ipInt1.longValue());
            Number ipInt2 = (Number) srcIPMapDB.get("ipInt");
            srcIPMapDB.put("ipInt", ipInt2.longValue());
            assertEquals(srcIPMap, srcIPMapDB);

            //add/get src port vert
            Map<String, Object> srcPortMap = conn.jsonVertToMap(vertices.getJSONObject(srcPortID));
            Map<String, Object> srcPortMapDB = conn.getVertByID(srcPortID);     
            assertEquals(srcPortMap, srcPortMapDB);

            //add/get dest address vert
            Map<String, Object> destAddressMap = conn.jsonVertToMap(vertices.getJSONObject(destAddressID));
            Map<String, Object> destAddressMapDB = conn.getVertByID(destAddressID);
            assertEquals(destAddressMap, destAddressMapDB);

            //add/get dest ip vert
            Map<String, Object> destIPMap = conn.jsonVertToMap(vertices.getJSONObject(destIPID));
            Map<String, Object> destIPMapDB = conn.getVertByID(destIPID);
            ipInt1 = (Number) destIPMap.get("ipInt");
            destIPMap.put("ipInt", ipInt1.longValue());
            ipInt2 = (Number) destIPMapDB.get("ipInt");
            destIPMapDB.put("ipInt", ipInt2.longValue());
            assertEquals(destIPMap, destIPMapDB); 

            //add/get dest port vert
            Map<String, Object> destPortMap = conn.jsonVertToMap(vertices.getJSONObject(destPortID));
            Map<String, Object> destPortMapID = conn.getVertByID(destPortID);
            assertEquals(destPortMap, destPortMapID);

            //getOutEdges
            List<Map<String, Object>> flowOutEdges = conn.getOutEdges(flowID);
            Map<String, Object> edge1 = convertEdgeToMap(edges.getJSONObject(0));
            Map<String, Object> edge2 = convertEdgeToMap(edges.getJSONObject(1));
            assertTrue(flowOutEdges.contains(edge1));
            assertTrue(flowOutEdges.contains(edge2));

            List<Map<String, Object>> srcAddressOutEdges = conn.getOutEdges(srcAddressID);
            Map<String, Object> edge3 = convertEdgeToMap(edges.getJSONObject(2));
            Map<String, Object> edge4 = convertEdgeToMap(edges.getJSONObject(3));
            assertTrue(srcAddressOutEdges.contains(edge3));
            assertTrue(srcAddressOutEdges.contains(edge4));

            List<Map<String, Object>> destAddressOutEdges = conn.getOutEdges(destAddressID);
            Map<String, Object> edge5 = convertEdgeToMap(edges.getJSONObject(4));
            Map<String, Object> edge6 = convertEdgeToMap(edges.getJSONObject(5));
            assertTrue(destAddressOutEdges.contains(edge5));
            assertTrue(destAddressOutEdges.contains(edge6));

            //getInEdges
            List<Map<String, Object>> srcAddressInEdges = conn.getInEdges(srcAddressID);
            assertTrue(srcAddressInEdges.size() == 1);
            assertTrue(srcAddressInEdges.contains(edge1));

            List<Map<String, Object>> destAddressInEdges = conn.getInEdges(destAddressID);
            assertTrue(destAddressInEdges.size() == 1);
            assertTrue(destAddressInEdges.contains(edge2));

            List<Map<String, Object>> srcIPInEdges = conn.getInEdges(srcIPID);
            assertTrue(srcIPInEdges.size() == 1);
            assertTrue(srcIPInEdges.contains(edge3));

            List<Map<String, Object>> destIPInEdges = conn.getInEdges(destIPID);
            assertTrue(destIPInEdges.size() == 1);
            assertTrue(destIPInEdges.contains(edge5)); 

            List<Map<String, Object>> srcPortInEdges = conn.getInEdges(srcPortID);
            assertTrue(srcPortInEdges.size() == 1);
            assertTrue(srcPortInEdges.contains(edge4));

            List<Map<String, Object>> destPortInEdges = conn.getInEdges(destPortID);
            assertTrue(destPortInEdges.size() == 1);
            assertTrue(destPortInEdges.contains(edge6));

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
            flowMap.put("sourceDocument", "some new sourceDocument");
            conn.updateVertex(flowID, flowMap);
            Map<String, Object> newFlow = conn.getVertByID(flowID);
            assertEquals(newFlow.get("sourceDocument"), "some new sourceDocument");

            //void removeEdgeByRelation(String inVertID, String outVertID, String relation)
            conn.removeEdgeByRelation(destAddressID, flowID, "Sub-Observable");
            conn.removeEdgeByRelation(destIPID, destAddressID, "Sub-Observable");
        } catch (RuntimeException e) {
            e.printStackTrace();
        }

        for (String id : (Set<String>) graph.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
    }

    /*
     * 210 MB file with Observables
     */
    // @Test
    public void testLoadLargeGraph() {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testLoadLargeGraph()");

        long startTime = System.currentTimeMillis();
        String[] HEADERS = {"_id","timestamp","modifieddate","vertexType","observableType","name","alias","description","source","sourceDocument"};
        // String[] HEADERS = {"_id","timestamp","modifieddate","vertexType","name","description","shortDescription","source","sourceDocument", "publishedDate"};
        CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(HEADERS);
        JSONObject vertices = new JSONObject();
        try {
        // CSVParser csvParser = new CSVParser(new FileReader("/Users/yv4/Documents/stucco/graph-db-connection-service/graph-db-connection/src/test/resources/vulnerability.csv"), csvFormat);
        CSVParser csvParser = new CSVParser(new FileReader("/Users/yv4/Documents/stucco/graph-db-connection-service/graph-db-connection/src/test/resources/data/observable.csv"), csvFormat);
        List<CSVRecord> records = csvParser.getRecords();
            for (int i = 1; i < records.size(); i++) {
            //for (int i = 1; i < 1000; i++) {
                CSVRecord record = records.get(i);
                String _id = record.get("_id");
                JSONObject vert = new JSONObject();
                for (int j = 1; j < HEADERS.length; j++) {
                    if (!record.get(HEADERS[j]).isEmpty()) {
                        if (HEADERS[j].equals("alias") || HEADERS[j].equals("description") || HEADERS[j].equals("source") || HEADERS[j].equals("shortDescription")) {
                            String str = record.get(HEADERS[j]);
                            // str = str.replaceAll("[{}\"]", "");
                            String[] aa = str.split(",");
                            JSONArray array = new JSONArray();
                            for (String a : aa) {
                                array.put(a);
                            }
                            vert.put(HEADERS[j], array);
                        } else {
                            vert.put(HEADERS[j], record.get(HEADERS[j]));
                        }
                    }
                }
                // vert.put("sourceDocument", "source document");
                vertices.put(_id, vert);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        JSONObject graph = new JSONObject();
        graph.put("vertices", vertices);
        conn.bulkLoadGraph(graph);
        System.out.println("count = " + conn.getVertCount());

        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println(totalTime);
    }
}