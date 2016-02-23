package gov.pnnl.stucco.dbconnect;



/**
 * This interface is specifically for building the index 
 *
 */
public interface DBConnectionIndexerInterface extends DBConnectionAlignment {

    /**
     * Builds the index for a set of properties per their specification
     * @param indexConfig
     * @throws Exception 
     */
    public void buildIndex(String indexConfig) throws Exception;
}
