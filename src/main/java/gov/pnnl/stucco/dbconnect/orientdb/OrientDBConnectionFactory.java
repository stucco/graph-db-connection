package gov.pnnl.stucco.dbconnect.orientdb;

import gov.pnnl.stucco.dbconnect.DBConnectionAlignment;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionIndexerInterface;
import gov.pnnl.stucco.dbconnect.DBConnectionTestInterface;

/**
 * This is a concrete Factory for the OrientDB instance of a DB
 *
 */
public class OrientDBConnectionFactory extends DBConnectionFactory {
    
    
    /**
     * constructor of the factory
     */
    public OrientDBConnectionFactory() {
        super();
    }
    
    /**
     * return the DBConnection object for Alignment uses
     */
    public DBConnectionAlignment getDBConnectionAlignment() {

        return new OrientDBConnection(configuration);

    }


    /**
     * return the DBconnection object for the Indexer
     * @return
     */
    @Override
    public DBConnectionIndexerInterface getDBConnectionIndexer() {
        
        return new OrientDBConnection(configuration);
    }

    /**
     * return the DBconnection object used for Testing
     */
    @Override
    public DBConnectionTestInterface getDBConnectionTestInterface() {
        return new OrientDBConnection(configuration);
    }

}
