package gov.pnnl.stucco.dbconnect.titan;

import gov.pnnl.stucco.dbconnect.DBConnectionAlignment;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionIndexerInterface;
import gov.pnnl.stucco.dbconnect.DBConnectionTestInterface;

/**
 * This is a concrete Factory for the Titan instance of a DB
 *
 */
public class TitanDBConnectionFactory extends DBConnectionFactory {
    
    
    /**
     * constructor of the factory
     */
    public TitanDBConnectionFactory() {
        super();
    }
    
    /**
     * return the DBConnection object for Alignment uses
     */
    public DBConnectionAlignment getDBConnectionAlignment() {

        return new TitanDBConnection(configuration);

    }


    /**
     * return the DBconnection object for the Indexer
     * @return
     */
    @Override
    public DBConnectionIndexerInterface getDBConnectionIndexer() {
        
        return new TitanDBConnection(configuration);
    }

    /**
     * return the DBconnection object used for Testing
     */
    @Override
    public DBConnectionTestInterface getDBConnectionTestInterface() {
        return new TitanDBConnection(configuration);
    }

}
