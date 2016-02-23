package gov.pnnl.stucco.dbconnect;

import java.util.Map;

import org.json.JSONObject;

/**
 * This interface is specifically for testing the functionality in the other interfaces of DBConnection
 *
 */
//public interface DBConnectionTestInterface extends DBConnectionAlignment {
public interface DBConnectionTestInterface extends DBConnectionIndexerInterface {


    /**
     * Replaces current vertex's property map with a different one
     * @param id - vertex that will be changed
     * @param properties - property map with different contents, complete replacement of current content
     */
    public void updateVertex(String id, Map<String, Object> properties);
    
    /**
     * converts JSON Vertices to a Map format
     * @param v - JSON object
     * @return a map of key value pairs
     */
    public Map<String, Object> jsonVertToMap(JSONObject v);
    
    /**
     * remove all vertices in the databases
     */
    public void removeAllVertices();
}
