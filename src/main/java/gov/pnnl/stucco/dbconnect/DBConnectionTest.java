package gov.pnnl.stucco.dbconnect;

import java.util.Map;

// GN: If there's only going to be one method in this, I'd probably just
// add it to DBConnectionAlignment instead.
public interface DBConnectionTest {

    /**
     * Replaces current vertex's property map with a different one
     * @param id - vertex that will be changed
     * @param properties - property map with different contents, complete replacement of current content
     */
    public void updateVertex(String id, Map<String, Object> properties);


}
