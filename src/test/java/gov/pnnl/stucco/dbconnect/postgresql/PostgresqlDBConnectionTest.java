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

    @Test
    public void testIncidentThreatActorTTPObservableDuplicate () {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testIncidentThreatActor()");

        String graphString1 =
            "   { " +
            "       \"vertices\":{ " +
            "           \"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\":{ " +
            "               \"sourceDocument\":\"<ttp:TTP xmlns:ttp=\\\"http://stix.mitre.org/TTP-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\\\" xsi:type=\\\"ttp:TTPType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><ttp:Title>TTP Title<\\/ttp:Title><ttp:Behavior><ttp:Exploits><ttp:Exploit><ttp:Title>Exploit Title 1<\\/ttp:Title><ttp:Description>Exploit Description 1<\\/ttp:Description><\\/ttp:Exploit><\\/ttp:Exploits><\\/ttp:Behavior><\\/ttp:TTP>\", " +
            "               \"vertexType\":\"Exploit\", " +
            "               \"name\":\"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\", " +
            "               \"description\":[\"Exploit Description 1\"] " +
            "           }, " +
            "           \"stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678\":{ " +
            "               \"sourceDocument\":\"<ta:Threat_Actor xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:ta=\\\"http://stix.mitre.org/ThreatActor-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678\\\" xsi:type=\\\"ta:ThreatActorType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><ta:Title>ThreatActor Title<\\/ta:Title><ta:Description>ThreatActor Description 1<\\/ta:Description><ta:Description>ThreatActor Description 2<\\/ta:Description><ta:Identity><stixCommon:Name>ThreatActor Name<\\/stixCommon:Name><stixCommon:Related_Identities><stixCommon:Related_Identity><stixCommon:Identity><stixCommon:Name>ThreatActor Related Name<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Related_Identity><\\/stixCommon:Related_Identities><\\/ta:Identity><\\/ta:Threat_Actor>\", " +
            "               \"vertexType\":\"Threat_Actor\", " +
            "               \"name\":\"ThreatActor Name\", " +
            "               \"description\":[\"ThreatActor Description 2\",\"ThreatActor Description 1\"] " +
            "           }, " +
            "           \"stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2\":{ " +
            "               \"sourceDocument\":\"<cybox:Observable xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:PortObj=\\\"http://cybox.mitre.org/objects#PortObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2\\\"><cybox:Object><cybox:Properties xsi:type=\\\"PortObj:PortObjectType\\\"><PortObj:Port_Value>80<\\/PortObj:Port_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\",  " +
            "               \"vertexType\":\"Observable\", " +
            "               \"name\":\"80\", " +
            "               \"observableType\":\"Port\" " +
            "           }, " +
            "           \"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\":{ " +
            "               \"sourceDocument\":\"<incident:Incident xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:ttp=\\\"http://stix.mitre.org/TTP-1\\\" xmlns:incident=\\\"http://stix.mitre.org/Incident-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:ta=\\\"http://stix.mitre.org/ThreatActor-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\\\" xsi:type=\\\"incident:IncidentType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><incident:Title>Incident Title<\\/incident:Title><incident:External_ID>External ID<\\/incident:External_ID><incident:Description>Incident description 1<\\/incident:Description><incident:Description>Incident description 2<\\/incident:Description><incident:Related_Observables><incident:Related_Observable><stixCommon:Observable idref=\\\"stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2\\\"><\\/stixCommon:Observable><\\/incident:Related_Observable><\\/incident:Related_Observables><incident:Leveraged_TTPs><incident:Leveraged_TTP><stixCommon:TTP idref=\\\"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\\\" xsi:type=\\\"ttp:TTPType\\\"><\\/stixCommon:TTP><\\/incident:Leveraged_TTP><\\/incident:Leveraged_TTPs><incident:Attributed_Threat_Actors><incident:Threat_Actor><stixCommon:Threat_Actor idref=\\\"stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678\\\" xsi:type=\\\"ta:ThreatActorType\\\"><\\/stixCommon:Threat_Actor><\\/incident:Threat_Actor><\\/incident:Attributed_Threat_Actors><incident:Information_Source><stixCommon:Identity><stixCommon:Name>Source Name 1<\\/stixCommon:Name><\\/stixCommon:Identity><stixCommon:Contributing_Sources><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Name 2<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Name 3<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><\\/stixCommon:Contributing_Sources><\\/incident:Information_Source><\\/incident:Incident>\", " +
            "               \"vertexType\":\"Incident\", " +
            "               \"name\":\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\", " +
            "               \"description\":[\"Incident description 1\",\"Incident description 2\"], " +
            "               \"source\":[\"Source Name 1\",\"Source Name 2\",\"Source Name 3\"] " +
            "           } " +
            "       }, " +
            "       \"edges\":[ " +
            "           { " +
            "                \"outVertID\":\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\", " +
            "                \"inVertID\":\"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\", " +
            "                \"outVertTable\":\"Incident\", " +
            "                \"inVertTable\":\"TTP\", " +
            "                \"relation\":\"LeveragedTTP\" " +
            "            }, " +
            "           { " +
            "                \"outVertID\":\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\", " +
            "                \"inVertID\":\"stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2\", " +
            "                \"outVertTable\":\"Incident\", " +
            "                \"inVertTable\":\"Observable\", " +
            "                \"relation\":\"RelatedObservable\" " +
            "            }, " +
            "           { " +
            "                \"outVertID\":\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\", " +
            "                \"inVertID\":\"stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678\", " +
            "                \"outVertTable\":\"Incident\", " +
            "                \"inVertTable\":\"Threat_Actor\", " +
            "                \"relation\":\"RelatedThreatActor\" " +
            "            } " +
            "       ] " +
            "   }";

        String graphString2 =
            "   { " +
            "       \"vertices\":{ " +
            "           \"stucco:port-123456-962a-408e-9495-be65b11fff09\": {"+
            "               \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:port-6e8e3e78-962a-408e-9495-be65b11fff09\\\"><cybox:Title>Port<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:port-56867\\\"><cybox:Description>80<\\/cybox:Description><cybox:Properties xmlns:PortObj=\\\"http://cybox.mitre.org/objects#PortObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"PortObj:PortObjectType\\\"><PortObj:Port_Value>56867<\\/PortObj:Port_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
            "               \"vertexType\": \"Observable\", " +
            "               \"name\": \"80\", " +
            "               \"description\": [\"80\"], " +
            "               \"source\": [\"Argus\"], " +
            "               \"observableType\": \"Port\" " +
            "           }," +
            "           \"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\": {"+
            "               \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" id=\\\"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\\\"><cybox:Title>Address<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">Argus<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:address-168430180_22\\\"><cybox:Description>10.10.10.100, port 80<\\/cybox:Description><cybox:Properties xmlns:SocketAddressObj=\\\"http://cybox.mitre.org/objects#SocketAddressObject-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" xsi:type=\\\"SocketAddressObj:SocketAddressObjectType\\\"><SocketAddressObj:IP_Address object_reference=\\\"stucco:ip-a5dff0b3-0f2f-4308-a16d-949c5826cf1a\\\" /><SocketAddressObj:Port object_reference=\\\"stucco:port-123456-962a-408e-9495-be65b11fff09\\\" /><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
            "               \"vertexType\": \"Observable\", " +
            "               \"name\": \"10.10.10.100:80\", " +
            "               \"description\": [\"10.10.10.100, port 80\"], " +
            "               \"source\": [\"Argus\"], " +
            "               \"observableType\": \"Socket Address\" " +
            "           }}," +
            "       \"edges\": ["+
            "           {"+
            "               \"outVertID\": \"stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd\","+
            "               \"inVertID\": \"stucco:port-123456-962a-408e-9495-be65b11fff09\","+
            "               \"outVertTable\": \"Observable\","+
            "               \"inVertTable\": \"Observable\","+
            "               \"relation\": \"Sub-Observable\""+
            "           }"+
            "       ]"+
            "   }";

        JSONObject graph1 = new JSONObject(graphString1);
        JSONObject vertices = graph1.getJSONObject("vertices");
        JSONArray edges = graph1.getJSONArray("edges");

        conn.bulkLoadGraph(graph1);

        String ttpID = "stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b";
        String taID = "stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678";
        String observableID = "stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2";
        String incidentID = "stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129";

        Map<String, Object> ttp = conn.jsonVertToMap(vertices.getJSONObject(ttpID));
        Map<String, Object> ttpDB = conn.getVertByID(ttpID);
        assertEquals(ttp, ttpDB);

        Map<String, Object> ta = conn.jsonVertToMap(vertices.getJSONObject(taID));
        Map<String, Object> taDB = conn.getVertByID(taID);
        assertEquals(ta, taDB);

        Map<String, Object> observable = conn.jsonVertToMap(vertices.getJSONObject(observableID));
        Map<String, Object> observableDB = conn.getVertByID(observableID);
        assertEquals(ttp, ttpDB);

        Map<String, Object> incident = conn.jsonVertToMap(vertices.getJSONObject(incidentID));
        Map<String, Object> incidentpDB = conn.getVertByID(incidentID);
        assertEquals(incident, incidentpDB);

        Map<String, Object> edge0 = convertEdgeToMap(edges.getJSONObject(0));
        Map<String, Object> edge1 = convertEdgeToMap(edges.getJSONObject(1));
        Map<String, Object> edge2 = convertEdgeToMap(edges.getJSONObject(2));

        List<Map<String, Object>> outEdges = conn.getOutEdges(incidentID);
        assertTrue(outEdges.contains(edge0));
        assertTrue(outEdges.contains(edge1));
        assertTrue(outEdges.contains(edge2));

        JSONObject graph2 = new JSONObject(graphString2);
        vertices = graph2.getJSONObject("vertices");
        edges = graph2.getJSONArray("edges");

        conn.bulkLoadGraph(graph2);

        Map<String, Object> updatedObservableDB = conn.getVertByID(observableID);
        assertEquals(updatedObservableDB.get("name"), "80");
        assertEquals(updatedObservableDB.get("vertexType"), "Observable");
        assertEquals(updatedObservableDB.get("observableType"), "Port");
        assertTrue(((Set<String>)updatedObservableDB.get("description")).contains("80"));
        assertTrue(((Set<String>)updatedObservableDB.get("source")).contains("Argus"));


        String duplicatePortID = "stucco:port-123456-962a-408e-9495-be65b11fff09";
        Map<String, Object> portDB = conn.getVertByID(duplicatePortID);
        assertNull(portDB);

        String addressID = "stucco:address-046baefe-f1d0-45ee-91c3-a9a22a7e6ddd";
        Map<String, Object> address = conn.jsonVertToMap(vertices.getJSONObject(addressID));
        Map<String, Object> addressDB = conn.getVertByID(addressID);
        String sourceDocument = (String)address.get("sourceDocument");
        String updatedSourceDocument = (String)addressDB.get("sourceDocument");
        assertNotEquals(sourceDocument, updatedSourceDocument);

        sourceDocument = sourceDocument.replaceAll(duplicatePortID, observableID);
        assertEquals(sourceDocument, updatedSourceDocument);

        List<Map<String, Object>> inEdge = conn.getInEdges(duplicatePortID);
        assertTrue(inEdge.isEmpty());


        for (String id : (Set<String>) graph1.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
        for (String id : (Set<String>) graph2.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
    }

    @Test
    public void testIncidentThreatActorObservableTTP () {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testIncidentThreatActorObservableTTP()");

        String graphString1 =
            "   { " +
            "       \"vertices\":{ " +
            "           \"stucco:Incident-19ef98a1-c297-4756-a99d-123456789100\":{ " +
            "               \"sourceDocument\":\"<incident:Incident xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:ttp=\\\"http://stix.mitre.org/TTP-1\\\" xmlns:incident=\\\"http://stix.mitre.org/Incident-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:ta=\\\"http://stix.mitre.org/ThreatActor-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:Incident-19ef98a1-c297-4756-a99d-123456789100\\\" xsi:type=\\\"incident:IncidentType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><incident:Title>Incident Title<\\/incident:Title><incident:External_ID>External ID<\\/incident:External_ID><incident:Description>Incident description 1<\\/incident:Description><incident:Description>Incident description 2<\\/incident:Description><incident:Related_Observables><incident:Related_Observable><stixCommon:Observable idref=\\\"stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2\\\"><\\/stixCommon:Observable><\\/incident:Related_Observable><\\/incident:Related_Observables><incident:Leveraged_TTPs><incident:Leveraged_TTP><stixCommon:TTP idref=\\\"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\\\" xsi:type=\\\"ttp:TTPType\\\"><\\/stixCommon:TTP><\\/incident:Leveraged_TTP><\\/incident:Leveraged_TTPs><incident:Attributed_Threat_Actors><incident:Threat_Actor><stixCommon:Threat_Actor idref=\\\"stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678\\\" xsi:type=\\\"ta:ThreatActorType\\\"><\\/stixCommon:Threat_Actor><\\/incident:Threat_Actor><\\/incident:Attributed_Threat_Actors><incident:Information_Source><stixCommon:Identity><stixCommon:Name>Source Name 1<\\/stixCommon:Name><\\/stixCommon:Identity><stixCommon:Contributing_Sources><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Name 2<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Name 3<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><\\/stixCommon:Contributing_Sources><\\/incident:Information_Source><\\/incident:Incident>\", " +
            "               \"vertexType\":\"Incident\", " +
            "               \"name\":\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\", " +
            "               \"source\":[\"Source Name 1\",\"Source Name 2\",\"Source Name 3\"] " +
            "           } " +
            "       }, " +
            "       \"edges\":[] " +
            "   }";

        String graphString2 =
            "   { " +
            "       \"vertices\":{ " +
            "           \"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\":{ " +
            "               \"sourceDocument\":\"<ttp:TTP xmlns:ttp=\\\"http://stix.mitre.org/TTP-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\\\" xsi:type=\\\"ttp:TTPType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><ttp:Title>TTP Title<\\/ttp:Title><ttp:Behavior><ttp:Exploits><ttp:Exploit><ttp:Title>Exploit Title 1<\\/ttp:Title><ttp:Description>Exploit Description 1<\\/ttp:Description><\\/ttp:Exploit><\\/ttp:Exploits><\\/ttp:Behavior><\\/ttp:TTP>\", " +
            "               \"vertexType\":\"Exploit\", " +
            "               \"name\":\"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\", " +
            "               \"description\":[\"Exploit Description 1\"] " +
            "           }, " +
            "           \"stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678\":{ " +
            "               \"sourceDocument\":\"<ta:Threat_Actor xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:ta=\\\"http://stix.mitre.org/ThreatActor-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678\\\" xsi:type=\\\"ta:ThreatActorType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><ta:Title>ThreatActor Title<\\/ta:Title><ta:Description>ThreatActor Description 1<\\/ta:Description><ta:Description>ThreatActor Description 2<\\/ta:Description><ta:Identity><stixCommon:Name>ThreatActor Name<\\/stixCommon:Name><stixCommon:Related_Identities><stixCommon:Related_Identity><stixCommon:Identity><stixCommon:Name>ThreatActor Related Name<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Related_Identity><\\/stixCommon:Related_Identities><\\/ta:Identity><\\/ta:Threat_Actor>\", " +
            "               \"vertexType\":\"Threat_Actor\", " +
            "               \"name\":\"ThreatActor Name\", " +
            "               \"description\":[\"ThreatActor Description 2\",\"ThreatActor Description 1\"] " +
            "           }, " +
            "           \"stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2\":{ " +
            "               \"sourceDocument\":\"<cybox:Observable xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:PortObj=\\\"http://cybox.mitre.org/objects#PortObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2\\\"><cybox:Object><cybox:Properties xsi:type=\\\"PortObj:PortObjectType\\\"><PortObj:Port_Value>80<\\/PortObj:Port_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\", " +
            "               \"vertexType\":\"Observable\", " +
            "               \"name\":\"80\", " +
            "               \"observableType\":\"Port\" " +
            "           }, " +
            "           \"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\":{ " +
            "               \"sourceDocument\":\"<incident:Incident xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:ttp=\\\"http://stix.mitre.org/TTP-1\\\" xmlns:incident=\\\"http://stix.mitre.org/Incident-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:ta=\\\"http://stix.mitre.org/ThreatActor-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\\\" xsi:type=\\\"incident:IncidentType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><incident:Title>Incident Title<\\/incident:Title><incident:External_ID>External ID<\\/incident:External_ID><incident:Description>Incident description 1<\\/incident:Description><incident:Description>Incident description 2<\\/incident:Description><incident:Related_Observables><incident:Related_Observable><stixCommon:Observable idref=\\\"stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2\\\"><\\/stixCommon:Observable><\\/incident:Related_Observable><\\/incident:Related_Observables><incident:Leveraged_TTPs><incident:Leveraged_TTP><stixCommon:TTP idref=\\\"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\\\" xsi:type=\\\"ttp:TTPType\\\"><\\/stixCommon:TTP><\\/incident:Leveraged_TTP><\\/incident:Leveraged_TTPs><incident:Attributed_Threat_Actors><incident:Threat_Actor><stixCommon:Threat_Actor idref=\\\"stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678\\\" xsi:type=\\\"ta:ThreatActorType\\\"><\\/stixCommon:Threat_Actor><\\/incident:Threat_Actor><\\/incident:Attributed_Threat_Actors><incident:Information_Source><stixCommon:Identity><stixCommon:Name>Source Name 1<\\/stixCommon:Name><\\/stixCommon:Identity><stixCommon:Contributing_Sources><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Name 2<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Name 3<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><\\/stixCommon:Contributing_Sources><\\/incident:Information_Source><\\/incident:Incident>\", " +
            "               \"vertexType\":\"Incident\", " +
            "               \"name\":\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\", " +
            "               \"description\":[\"Incident description 1\",\"Incident description 2\"], " +
            "               \"source\":[\"Source Name 1\",\"Source Name 2\",\"Source Name 3\"] " +
            "           } " +
            "       }, " +
            "       \"edges\":[ " +
            "           { " +
            "               \"outVertTable\":\"Incident\", " +
            "               \"outVertID\":\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\", " +
            "               \"inVertTable\":\"Exploit\", " +
            "               \"inVertID\":\"stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b\", " +
            "               \"relation\":\"LeveragedTTP\" " +
            "           }, " +
            "           { " +
            "               \"outVertTable\":\"Incident\", " +
            "               \"outVertID\":\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\", " +
            "               \"inVertTable\":\"Observable\", " +
            "               \"inVertID\":\"stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2\", " +
            "               \"relation\":\"RelatedObservable\" " +
            "           }, " +
            "           { " +
            "               \"outVertTable\":\"Incident\", " +
            "               \"outVertID\":\"stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129\", " +
            "               \"inVertTable\":\"Threat_Actor\", " +
            "               \"inVertID\":\"stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678\", " +
            "               \"relation\":\"RelatedThreatActor\" " +
            "           } " +
            "       ] " +
            "   }";

        JSONObject graph1 = new JSONObject(graphString1);
        JSONObject vertices1 = graph1.getJSONObject("vertices");

        JSONObject graph2 = new JSONObject(graphString2);
        JSONObject vertices2 = graph2.getJSONObject("vertices");
        JSONArray edges2 = graph2.getJSONArray("edges");

        String incidentID = "stucco:Incident-19ef98a1-c297-4756-a99d-123456789100";
        String incidentDuplID = "stucco:Incident-19ef98a1-c297-4756-a99d-43b885ef0129";
        String exploitID = "stucco:TTP-40d40e02-7055-43d3-8e7e-f566e4f53a3b";
        String observableID = "stucco:Observable-d4a1b891-2905-49b9-90af-1c614528cdb2";
        String taID = "stucco:ThreatActors-3aaad08a-ea7a-44e1-b809-7d3d6b5f3678";

        conn.bulkLoadGraph(graph1);

        Map<String, Object> incident = conn.jsonVertToMap(vertices1.getJSONObject(incidentID));
        Map<String, Object> incidentDB = conn.getVertByID(incidentID);
        assertEquals(incident, incidentDB);

        conn.bulkLoadGraph(graph2);

        Map<String, Object> exploit = conn.jsonVertToMap(vertices2.getJSONObject(exploitID));
        Map<String, Object> exploitDB = conn.getVertByID(exploitID);
        assertEquals(exploit, exploitDB);

        Map<String, Object> observable = conn.jsonVertToMap(vertices2.getJSONObject(observableID));
        Map<String, Object> observableDB = conn.getVertByID(observableID);
        assertEquals(observable, observableDB);

        Map<String, Object> ta = conn.jsonVertToMap(vertices2.getJSONObject(taID));
        Map<String, Object> taDB = conn.getVertByID(taID);
        assertEquals(ta, taDB);

        /* testing merge of description */
        Set<String> description = new HashSet<String>();
        description.add("Incident description 1");
        description.add("Incident description 2");
        incident.put("description", (Object)description);

        incidentDB = conn.getVertByID(incidentID);
        assertEquals(incident, incidentDB);

        for (int i = 0; i < edges2.length(); i++) {
            JSONObject edge = edges2.getJSONObject(i);
            edge.put("outVertID", "stucco:Incident-19ef98a1-c297-4756-a99d-123456789100");
        }

        List<Map<String, Object>> outEdges = conn.getOutEdges(incidentDuplID);
        assertTrue(outEdges.isEmpty());

        outEdges = conn.getOutEdges(incidentID);

        assertTrue(outEdges.contains(convertEdgeToMap(edges2.getJSONObject(0))));
        assertTrue(outEdges.contains(convertEdgeToMap(edges2.getJSONObject(1))));
        assertTrue(outEdges.contains(convertEdgeToMap(edges2.getJSONObject(2))));



        for (String id : (Set<String>) graph1.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
        for (String id : (Set<String>) graph2.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
    }

     @Test
    public void testCampaingDuplicateByAlias () {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testCampaingDuplicateByAlias()");

        String graphString1 =
            "   { " +
            "       \"vertices\":{ " +
            "           \"stucco:campaign-a2dec921-6a3f-49e4-b415-original\":{ " +
            "               \"sourceDocument\":\"<campaign:Campaign xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:campaign=\\\"http://stix.mitre.org/Campaign-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:ta=\\\"http://stix.mitre.org/ThreatActor-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:campaign-a2dec921-6a3f-49e4-b415-402b376fff5c\\\" xsi:type=\\\"campaign:CampaignType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><campaign:Title>Campaign<\\/campaign:Title><campaign:Description>Campaign description<\\/campaign:Description><campaign:Names><campaign:Name>Campaigns Name<\\/campaign:Name><\\/campaign:Names><campaign:Attribution><campaign:Attributed_Threat_Actor><stixCommon:Threat_Actor idref=\\\"stucco:threat-9f055e12-d799-47d8-84a5-f018ee1ccb99\\\" xsi:type=\\\"ta:ThreatActorType\\\"/><\\/campaign:Attributed_Threat_Actor><\\/campaign:Attribution><campaign:Information_Source><stixCommon:Identity><stixCommon:Name>Source One<\\/stixCommon:Name><\\/stixCommon:Identity><stixCommon:Contributing_Sources><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Two<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><\\/stixCommon:Contributing_Sources><\\/campaign:Information_Source><\\/campaign:Campaign>\", " +
            "               \"vertexType\":\"Campaign\", " +
            "               \"name\":\"stucco:campaign-a2dec921-6a3f-49e4-b415-402b376fff5c\", " +
            "               \"description\":[\"Campaign description\"], " +
            "               \"alias\":[\"Campaign One\"], " +
            "               \"source\":[\"Source One\",\"Source Two\"] " +
            "           } " +
            "       }, " +
            "       \"edges\":[] " +
            "   }";

        String graphString2 =
            "   { " +
            "       \"vertices\":{ " +
            "           \"stucco:campaign-a2dec921-6a3f-49e4-b415-duplicate\":{ " +
            "               \"sourceDocument\":\"<campaign:Campaign xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:campaign=\\\"http://stix.mitre.org/Campaign-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:ta=\\\"http://stix.mitre.org/ThreatActor-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:campaign-a2dec921-6a3f-49e4-b415-402b376fff5c\\\" xsi:type=\\\"campaign:CampaignType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><campaign:Title>Campaign<\\/campaign:Title><campaign:Description>Campaign description<\\/campaign:Description><campaign:Names><campaign:Name>Campaigns Name<\\/campaign:Name><\\/campaign:Names><campaign:Attribution><campaign:Attributed_Threat_Actor><stixCommon:Threat_Actor idref=\\\"stucco:threat-9f055e12-d799-47d8-84a5-f018ee1ccb99\\\" xsi:type=\\\"ta:ThreatActorType\\\"/><\\/campaign:Attributed_Threat_Actor><\\/campaign:Attribution><campaign:Information_Source><stixCommon:Identity><stixCommon:Name>Source One<\\/stixCommon:Name><\\/stixCommon:Identity><stixCommon:Contributing_Sources><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Two<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><\\/stixCommon:Contributing_Sources><\\/campaign:Information_Source><\\/campaign:Campaign>\", " +
            "               \"vertexType\":\"Campaign\", " +
            "               \"name\":\"stucco:campaign-a2dec921-6a3f-49e4-b415-duplicate\", " +
            "               \"description\":[\"Campaign description\"], " +
            "               \"alias\":[\"Campaign One\", \"Campaign Two\"], " +
            "               \"source\":[\"Source One\",\"Source Two\"] " +
            "           } " +
            "       }, " +
            "       \"edges\":[] " +
            "   }";

        String originalID = "stucco:campaign-a2dec921-6a3f-49e4-b415-original";
        String duplicateID = "stucco:campaign-a2dec921-6a3f-49e4-b415-duplicate";

        JSONObject graph1 = new JSONObject(graphString1);
        JSONObject vertices1 = graph1.getJSONObject("vertices");

        conn.bulkLoadGraph(graph1);

        Map<String, Object> original = conn.jsonVertToMap(vertices1.getJSONObject(originalID));
        Map<String, Object> originalDB = conn.getVertByID(originalID);
        assertEquals(original, originalDB);

        JSONObject graph2 = new JSONObject(graphString2);
        JSONObject vertices2 = graph2.getJSONObject("vertices");

        conn.bulkLoadGraph(graph2);

        Map<String, Object> duplicateDB = conn.getVertByID(duplicateID);
        assertNull(duplicateDB);

        Set<String> alias = (Set<String>)original.get("alias");
        alias.add("Campaign Two");
        original.put("alias", alias);

        originalDB = conn.getVertByID(originalID);
        assertEquals(original, originalDB);

        for (String id : (Set<String>) graph1.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
        for (String id : (Set<String>) graph2.getJSONObject("vertices").keySet()) {
            conn.removeVertByID(id);
        }
    }

    @Test
    public void testLoadAllStixElements () {
        System.out.println("RUNNING: gov.pnnl.stucco.dbconnect.postgresql.testLoadAllStixElements()");

        String graphString =
            "   { " +
            "       \"vertices\":{ " +
            "           \"stucco:indicator-f6c15754-2fe0-4b1e-a43a-8fc1df4e49ca\":{ " +
            "               \"sourceDocument\":\"<indicator:Indicator xmlns:indicator=\\\"http://stix.mitre.org/Indicator-2\\\" xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:ttp=\\\"http://stix.mitre.org/TTP-1\\\" xmlns:coa=\\\"http://stix.mitre.org/CourseOfAction-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:indicator-f6c15754-2fe0-4b1e-a43a-8fc1df4e49ca\\\" xsi:type=\\\"indicator:IndicatorType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><indicator:Title>Indicator<\\/indicator:Title><indicator:Description>Indicator description<\\/indicator:Description><indicator:Observable idref=\\\"stucco:observable-bb95c949-7720-4b16-a491-93e0453b2785\\\"><\\/indicator:Observable><indicator:Indicated_TTP><stixCommon:TTP idref=\\\"stucco:ttp-8da79cbe-750d-4426-b960-baf8e67ec714\\\" xsi:type=\\\"ttp:TTPType\\\"/><\\/indicator:Indicated_TTP><indicator:Suggested_COAs><indicator:Suggested_COA><stixCommon:Course_Of_Action idref=\\\"stucco:coa-ba3d4963-caa5-4f65-b224-8f0d5ab38aa7\\\" xsi:type=\\\"coa:CourseOfActionType\\\"/><\\/indicator:Suggested_COA><\\/indicator:Suggested_COAs><indicator:Related_Campaigns><indicator:Related_Campaign><stixCommon:Campaign idref=\\\"stucco:campaign-a2dec921-6a3f-49e4-b415-402b376fff5c\\\"><\\/stixCommon:Campaign><\\/indicator:Related_Campaign><\\/indicator:Related_Campaigns><indicator:Producer><stixCommon:Identity><stixCommon:Name>Source One<\\/stixCommon:Name><\\/stixCommon:Identity><stixCommon:Contributing_Sources><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Two<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><\\/stixCommon:Contributing_Sources><\\/indicator:Producer><\\/indicator:Indicator>\", " +
            "               \"vertexType\":\"Indicator\", " +
            "               \"name\":\"stucco:indicator-f6c15754-2fe0-4b1e-a43a-8fc1df4e49ca\", " +
            "               \"description\":[\"Indicator description\"], " +
            "               \"source\":[\"Source One\",\"Source Two\"] " +
            "           }, " +
            "           \"stucco:et-52962ed3-1c7f-4cac-bedb-d49bb429b625\":{ " +
            "               \"sourceDocument\":\"<et:Exploit_Target xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:coa=\\\"http://stix.mitre.org/CourseOfAction-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:et=\\\"http://stix.mitre.org/ExploitTarget-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:et-52962ed3-1c7f-4cac-bedb-d49bb429b625\\\" xsi:type=\\\"et:ExploitTargetType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><et:Title>Exploit_Target<\\/et:Title><et:Description>Description<\\/et:Description><et:Weakness><et:Description>Description of this weakness<\\/et:Description><et:CWE_ID>CWE-93487297<\\/et:CWE_ID><\\/et:Weakness><et:Potential_COAs><et:Potential_COA><stixCommon:Course_Of_Action idref=\\\"stucco:coa-ba3d4963-caa5-4f65-b224-8f0d5ab38aa7\\\" xsi:type=\\\"coa:CourseOfActionType\\\"><\\/stixCommon:Course_Of_Action><\\/et:Potential_COA><\\/et:Potential_COAs><et:Information_Source><stixCommon:Identity><stixCommon:Name>Source One<\\/stixCommon:Name><\\/stixCommon:Identity><stixCommon:Contributing_Sources><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Two<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><\\/stixCommon:Contributing_Sources><\\/et:Information_Source><\\/et:Exploit_Target>\", " +
            "               \"vertexType\":\"Weakness\", " +
            "               \"name\":\"CWE-93487297\", " +
            "               \"description\":[\"Description of this weakness\"], " +
            "               \"source\":[\"Source One\",\"Source Two\"] " +
            "           }, " +
            "           \"stucco:ttp-8da79cbe-750d-4426-b960-baf8e67ec714\":{ " +
            "               \"sourceDocument\":\"<ttp:TTP xmlns:ttp=\\\"http://stix.mitre.org/TTP-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:ttp-8da79cbe-750d-4426-b960-baf8e67ec714\\\" xsi:type=\\\"ttp:TTPType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><ttp:Title>TTP<\\/ttp:Title><\\/ttp:TTP>\", " +
            "               \"vertexType\":\"TTP\", " +
            "               \"name\":\"stucco:ttp-8da79cbe-750d-4426-b960-baf8e67ec714\" " +
            "           }, " +
            "           \"stucco:observable-bb95c949-7720-4b16-a491-93e0453b2785\":{ " +
            "               \"sourceDocument\":\"<cybox:Observable xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" xmlns:PortObj=\\\"http://cybox.mitre.org/objects#PortObject-2\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:observable-bb95c949-7720-4b16-a491-93e0453b2785\\\"><cybox:Object><cybox:Properties xsi:type=\\\"PortObj:PortObjectType\\\"><PortObj:Port_Value>80<\\/PortObj:Port_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\",  " +
            "               \"vertexType\":\"Observable\", " +
            "               \"name\":\"80\", " +
            "               \"observableType\":\"Port\" " +
            "           }, " +
            "           \"stucco:threat-9f055e12-d799-47d8-84a5-f018ee1ccb99\":{ " +
            "               \"sourceDocument\":\"<ta:Threat_Actor xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:ta=\\\"http://stix.mitre.org/ThreatActor-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:threat-9f055e12-d799-47d8-84a5-f018ee1ccb99\\\" xsi:type=\\\"ta:ThreatActorType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><ta:Title>Threat_Actor<\\/ta:Title><ta:Identity><stixCommon:Name>Actor's name<\\/stixCommon:Name><stixCommon:Related_Identities><stixCommon:Related_Identity><stixCommon:Identity><stixCommon:Name>Related Name<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Related_Identity><\\/stixCommon:Related_Identities><\\/ta:Identity><\\/ta:Threat_Actor>\", " +
            "               \"vertexType\":\"Threat_Actor\", " +
            "               \"name\":\"Actor's name\", " +
            "           }, " +
            "           \"stucco:campaign-a2dec921-6a3f-49e4-b415-402b376fff5c\":{ " +
            "               \"sourceDocument\":\"<campaign:Campaign xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:campaign=\\\"http://stix.mitre.org/Campaign-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:ta=\\\"http://stix.mitre.org/ThreatActor-1\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:campaign-a2dec921-6a3f-49e4-b415-402b376fff5c\\\" xsi:type=\\\"campaign:CampaignType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><campaign:Title>Campaign<\\/campaign:Title><campaign:Description>Campaign description<\\/campaign:Description><campaign:Names><campaign:Name>Campaigns Name<\\/campaign:Name><\\/campaign:Names><campaign:Attribution><campaign:Attributed_Threat_Actor><stixCommon:Threat_Actor idref=\\\"stucco:threat-9f055e12-d799-47d8-84a5-f018ee1ccb99\\\" xsi:type=\\\"ta:ThreatActorType\\\"/><\\/campaign:Attributed_Threat_Actor><\\/campaign:Attribution><campaign:Information_Source><stixCommon:Identity><stixCommon:Name>Source One<\\/stixCommon:Name><\\/stixCommon:Identity><stixCommon:Contributing_Sources><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Two<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><\\/stixCommon:Contributing_Sources><\\/campaign:Information_Source><\\/campaign:Campaign>\", " +
            "               \"vertexType\":\"Campaign\", " +
            "               \"name\":\"stucco:campaign-a2dec921-6a3f-49e4-b415-402b376fff5c\", " +
            "               \"description\":[\"Campaign description\"], " +
            "               \"alias\":[\"Campaigns Name\"], " +
            "               \"source\":[\"Source One\",\"Source Two\"] " +
            "           }, " +
            "           \"stucco:coa-ba3d4963-caa5-4f65-b224-8f0d5ab38aa7\":{ " +
            "               \"sourceDocument\":\"<coa:Course_Of_Action xmlns:stixCommon=\\\"http://stix.mitre.org/common-1\\\" xmlns:coa=\\\"http://stix.mitre.org/CourseOfAction-1\\\" xmlns:stucco=\\\"gov.ornl.stucco\\\" xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" id=\\\"stucco:coa-ba3d4963-caa5-4f65-b224-8f0d5ab38aa7\\\" xsi:type=\\\"coa:CourseOfActionType\\\" xmlns=\\\"http://xml/metadataSharing.xsd\\\"><coa:Title>Course_Of_Action<\\/coa:Title><coa:Description>Course_Of_Action description<\\/coa:Description><coa:Information_Source><stixCommon:Identity><stixCommon:Name>Source One<\\/stixCommon:Name><\\/stixCommon:Identity><stixCommon:Contributing_Sources><stixCommon:Source><stixCommon:Identity><stixCommon:Name>Source Two<\\/stixCommon:Name><\\/stixCommon:Identity><\\/stixCommon:Source><\\/stixCommon:Contributing_Sources><\\/coa:Information_Source><\\/coa:Course_Of_Action>\", " +
            "               \"vertexType\":\"Course_Of_Action\", " +
            "               \"name\":\"stucco:coa-ba3d4963-caa5-4f65-b224-8f0d5ab38aa7\", " +
            "               \"description\":[\"Course_Of_Action description\"], " +
            "               \"source\":[\"Source One\",\"Source Two\"] " +
            "           } " +
            "       }, " +
            "       \"edges\":[ " +
            "           { " +
            "               \"outVertTable\":\"Campaign\", " +
            "               \"outVertID\":\"stucco:campaign-a2dec921-6a3f-49e4-b415-402b376fff5c\", " +
            "               \"inVertTable\":\"Threat_Actor\", " +
            "               \"inVertID\":\"stucco:threat-9f055e12-d799-47d8-84a5-f018ee1ccb99\", " +
            "               \"relation\":\"Attribution\" " +
            "           },{ " +
            "               \"outVertTable\":\"Indicator\", " +
            "               \"outVertID\":\"stucco:indicator-f6c15754-2fe0-4b1e-a43a-8fc1df4e49ca\", " +
            "               \"inVertTable\":\"Campaign\", " +
            "               \"inVertID\":\"stucco:campaign-a2dec921-6a3f-49e4-b415-402b376fff5c\", " +
            "               \"relation\":\"RelatedCampaign\" " +
            "           },{ " +
            "               \"outVertTable\":\"Indicator\", " +
            "               \"outVertID\":\"stucco:indicator-f6c15754-2fe0-4b1e-a43a-8fc1df4e49ca\", " +
            "               \"inVertTable\":\"Observable\", " +
            "               \"inVertID\":\"stucco:observable-bb95c949-7720-4b16-a491-93e0453b2785\", " +
            "               \"relation\":\"Observable\" " +
            "           },{ " +
            "               \"outVertTable\":\"Indicator\", " +
            "               \"outVertID\":\"stucco:indicator-f6c15754-2fe0-4b1e-a43a-8fc1df4e49ca\", " +
            "               \"inVertTable\":\"Course_Of_Action\", " +
            "               \"inVertID\":\"stucco:coa-ba3d4963-caa5-4f65-b224-8f0d5ab38aa7\", " +
            "               \"relation\":\"SuggestedCOA\" " +
            "           },{ " +
            "               \"outVertTable\":\"Indicator\", " +
            "               \"outVertID\":\"stucco:indicator-f6c15754-2fe0-4b1e-a43a-8fc1df4e49ca\", " +
            "               \"inVertTable\":\"TTP\", " +
            "               \"inVertID\":\"stucco:ttp-8da79cbe-750d-4426-b960-baf8e67ec714\", " +
            "               \"relation\":\"IndicatedTTP\" " +
            "           },{ " +
            "               \"outVertTable\":\"Weakness\", " +
            "               \"outVertID\":\"stucco:et-52962ed3-1c7f-4cac-bedb-d49bb429b625\", " +
            "               \"inVertTable\":\"Course_Of_Action\", " +
            "               \"inVertID\":\"stucco:coa-ba3d4963-caa5-4f65-b224-8f0d5ab38aa7\", " +
            "               \"relation\":\"PotentialCOA\" " +
            "           } " +
            "       ] " +
            "   }";

        JSONObject graph = new JSONObject(graphString);
        JSONObject vertices = graph.getJSONObject("vertices");
        JSONArray edges = graph.getJSONArray("edges");

        conn.bulkLoadGraph(graph);

        String indicatorID = "stucco:indicator-f6c15754-2fe0-4b1e-a43a-8fc1df4e49ca";
        String weaknessID = "stucco:et-52962ed3-1c7f-4cac-bedb-d49bb429b625";
        String ttpID = "stucco:ttp-8da79cbe-750d-4426-b960-baf8e67ec714";
        String observableID = "stucco:observable-bb95c949-7720-4b16-a491-93e0453b2785";
        String taID = "stucco:threat-9f055e12-d799-47d8-84a5-f018ee1ccb99";
        String campaignID = "stucco:campaign-a2dec921-6a3f-49e4-b415-402b376fff5c";
        String coaID = "stucco:coa-ba3d4963-caa5-4f65-b224-8f0d5ab38aa7";

        Map<String, Object> indicator = conn.jsonVertToMap(vertices.getJSONObject(indicatorID));
        Map<String, Object> indicatorDB = conn.getVertByID(indicatorID);
        assertEquals(indicator, indicatorDB);

        Map<String, Object> weakness = conn.jsonVertToMap(vertices.getJSONObject(weaknessID));
        Map<String, Object> weaknessDB = conn.getVertByID(weaknessID);
        assertEquals(weakness, weaknessDB);

        Map<String, Object> ttp = conn.jsonVertToMap(vertices.getJSONObject(ttpID));
        Map<String, Object> ttpDB = conn.getVertByID(ttpID);
        assertEquals(ttp, ttpDB);

        Map<String, Object> observable = conn.jsonVertToMap(vertices.getJSONObject(observableID));
        Map<String, Object> observableDB = conn.getVertByID(observableID);
        assertEquals(observable, observableDB);

        Map<String, Object> ta = conn.jsonVertToMap(vertices.getJSONObject(taID));
        Map<String, Object> taDB = conn.getVertByID(taID);
        assertEquals(ta, taDB);

        Map<String, Object> campaign = conn.jsonVertToMap(vertices.getJSONObject(campaignID));
        Map<String, Object> campaignDB = conn.getVertByID(campaignID);
        assertEquals(campaign, campaignDB);

        Map<String, Object> coa = conn.jsonVertToMap(vertices.getJSONObject(coaID));
        Map<String, Object> coaDB = conn.getVertByID(coaID);
        assertEquals(coa, coaDB);

        List<Map<String, Object>> outEdges = conn.getOutEdges(campaignID);
        assertTrue(outEdges.contains(convertEdgeToMap(edges.getJSONObject(0))));

        outEdges = conn.getOutEdges(indicatorID);
        assertTrue(outEdges.contains(convertEdgeToMap(edges.getJSONObject(1))));
        assertTrue(outEdges.contains(convertEdgeToMap(edges.getJSONObject(2))));
        assertTrue(outEdges.contains(convertEdgeToMap(edges.getJSONObject(3))));
        assertTrue(outEdges.contains(convertEdgeToMap(edges.getJSONObject(4))));

        outEdges = conn.getOutEdges(weaknessID);
        assertTrue(outEdges.contains(convertEdgeToMap(edges.getJSONObject(5))));

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