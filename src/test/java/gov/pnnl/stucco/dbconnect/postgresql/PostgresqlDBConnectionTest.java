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

        String id;
        try {
            conn.removeAllVertices();
            Map<String, Object> vertex = conn.jsonVertToMap(new JSONObject(vertexString));
            id = conn.addVertex(vertex);
            Map<String, Object> expectedVertex = conn.getVertByID(id);
            assertEquals(vertex, expectedVertex);
            long count = conn.getVertCount();
            System.out.println("vert count = " + count);
            count = conn.getEdgeCount();
            System.out.println("edge count = " + count);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
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
        try {
            conn.removeAllVertices();
            Map<String, Object> indicatorVertex = conn.jsonVertToMap(vertices.getJSONObject("Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402")); 
            String outVertID = conn.addVertex(indicatorVertex);
            Map<String, Object> ttpVertex = conn.jsonVertToMap(vertices.getJSONObject("TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70"));
            String inVertID = conn.addVertex(ttpVertex);
            conn.addEdge(inVertID, outVertID, "IndicatedTTP");
            Map<String, Object> expectedIndicatorVertex = conn.getVertByID(outVertID);
            assertEquals(indicatorVertex, expectedIndicatorVertex);
            Map<String, Object> expectedTTPVertex = conn.getVertByID(inVertID);
            assertEquals(ttpVertex, expectedTTPVertex);
            long count = conn.getVertCount();
            System.out.println("count = " + count);
            count = conn.getEdgeCount();
            List<Map<String, Object>> outEdges = conn.getOutEdges(outVertID);
            assertEquals(outEdges.size(), 1);
            Map<String, Object> outEdge = outEdges.get(0);
            assertEquals(outEdge.get("outVertID"), outVertID);
            assertEquals(outEdge.get("inVertID"), inVertID);
            assertEquals(outEdge.get("relation"), "IndicatedTTP");
            List<Map<String, Object>> inEdges = conn.getInEdges(inVertID);
            assertEquals(inEdges.size(), 1);
            assertEquals(outEdges, inEdges);
            conn.removeVertByID(outVertID);
            count = conn.getVertCount();
            assertEquals(count, 1);
            count = conn.getEdgeCount();
            assertEquals(count, 0);
            conn.removeVertByID(inVertID);
            count = conn.getVertCount();
            assertEquals(count, 0);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    public void testUpdateProperties() {
        String vertString =
            "{" +
            "      \"sourceDocument\": \"<cybox:Observable xmlns:cybox=\\\"http://cybox.mitre.org/cybox-2\\\" id=\\\"stucco:ip-cf1042ad-8f95-47e2-830d-4951f81f5241\\\"><cybox:Title>IP<\\/cybox:Title><cybox:Observable_Source><cyboxCommon:Information_Source_Type xmlns:cyboxCommon=\\\"http://cybox.mitre.org/common-2\\\">LoginEvent<\\/cyboxCommon:Information_Source_Type><\\/cybox:Observable_Source><cybox:Object id=\\\"stucco:ip-3232238091\\\"><cybox:Description>192.168.10.11<\\/cybox:Description><cybox:Properties xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\" category=\\\"ipv4-addr\\\" xsi:type=\\\"AddressObj:AddressObjectType\\\"><AddressObj:Address_Value xmlns:AddressObj=\\\"http://cybox.mitre.org/objects#AddressObject-2\\\">216.98.188.1<\\/AddressObj:Address_Value><\\/cybox:Properties><\\/cybox:Object><\\/cybox:Observable>\","+
            "      \"vertexType\": \"IP\"," +
            "      \"ipInt\": 3630349313," +
            "      \"name\": \"216.98.188.1\"," +
            "      \"observableType\": \"Address\"" +
            "}";
        try {
            conn.removeAllVertices();
            conn.addVertex(conn.jsonVertToMap(new JSONObject(vertString)));
            long count = conn.getVertCount();
            assertEquals(count, 1);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }
}


