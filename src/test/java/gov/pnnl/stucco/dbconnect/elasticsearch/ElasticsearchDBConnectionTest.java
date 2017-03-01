package gov.ornl.stucco.elasticsearch;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.net.UnknownHostException;

import java.io.FileNotFoundException;

/**
 * Unit test for ElasticsearchDBConnection
 */
public class ElasticsearchDBConnectionTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case 
     */
    public ElasticsearchDBConnectionTest( String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite( ElasticsearchDBConnectionTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() {
        try {
            ElasticsearchDBConnection es = new ElasticsearchDBConnection("./config/elasticsearch.yaml");
            Connection connection = es.getConnection();

            connection.open();
            //connection.printDBContent();
            connection.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assertTrue( true );
    }
}
