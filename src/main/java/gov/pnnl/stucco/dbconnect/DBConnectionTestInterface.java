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
     * converts JSON Vertices to a Map format
     * @param v - JSON object
     * @return a map of key value pairs
     */
    public Map<String, Object> jsonVertToMap(JSONObject v);

    /**
     * remove all vertices in the databases
     */
    public void removeAllVertices();

    /**
     * load db state from the specified file
     */
    public void loadState(String filePath);

    /**
     * save db state to the specified file
     */
    public void saveState(String filePath);
}
