package gov.pnnl.stucco.dbconnect.inmemory;

import gov.pnnl.stucco.dbconnect.DBConnectionAlignment;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionTestInterface;

/**
 * This is a concrete Factory for the in-memory instance of a DB
 *
 */
public class InMemoryDBConnectionFactory extends DBConnectionFactory {
    
    
    /**
     * constructor of the factory
     */
    public InMemoryDBConnectionFactory() {
        super();
    }
    
    /**
     * return the DBConnection object for Alignment uses
     */
    public DBConnectionAlignment getDBConnectionAlignment() {

        return new InMemoryDBConnection();

    }

    /**
     * return the DBconnection object for the Indexer
     * @return
     */
    public DBConnectionAlignment getDBConnectionIndexer() {

        // TODO
        return null;
    }

    /**
     * return the DBconnection object used for Testing
     */
    @Override
    public DBConnectionTestInterface getDBConnectionTestInterface() {
        return new InMemoryDBConnection();
    }

}
