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

import junit.framework.TestCase;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.json.JSONObject;

/**
 * Unit test for generically Testing the DBConnection
 * NOTE: two environment variable must be defined:
 *       STUCCO_DB_CONFIG=<path/filename.yml>
 *       STUCCO_DB_TYPE= INMEMORY|ORIENTDB|TITAN|NEO4J
 */
public class PostgresqlDBConnectionTest extends TestCase {
    private static DBConnectionFactory factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.POSTGRESQL);
    private static DBConnectionTestInterface conn;

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
//    public PostgresqlDBConnectionTest(String testName) {
//        super(testName);
//    }

    public void setUp() {
        factory.setConfiguration("./config/postgresql.yml");
        conn = factory.getDBConnectionTestInterface();
        conn.open();
    }

    public void tearDown(){
        conn.removeAllVertices();
        conn.close();
    }

    public void testLoadVertex() {
        String vertex = 
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
            id = conn.addVertex(conn.jsonVertToMap(new JSONObject(vertex)));
            System.out.println("looking for = " + id);
            conn.getVertByID(id);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        assertTrue(true);
      
    }

    public void testTwo() {
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
            String outVertID = conn.addVertex(conn.jsonVertToMap(vertices.getJSONObject("Indicator-a32549e9-02ea-4891-8f4d-e3b0412ac402")));
            String inVertID = conn.addVertex(conn.jsonVertToMap(vertices.getJSONObject("TTP-e94f0d8c-8f73-41a6-a834-9bcada3d3c70")));
            System.out.println(outVertID);
            System.out.println(inVertID);
            conn.addEdge(inVertID, outVertID, "IndicatedTTP");
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }
}


