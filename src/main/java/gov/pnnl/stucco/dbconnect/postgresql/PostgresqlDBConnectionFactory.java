package gov.pnnl.stucco.dbconnect.postgresql;

import gov.pnnl.stucco.dbconnect.DBConnectionAlignment;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionIndexerInterface;
import gov.pnnl.stucco.dbconnect.DBConnectionTestInterface;

/**
 * Factory for the Postgresql DB
 *
 */
public class PostgresqlDBConnectionFactory extends DBConnectionFactory {
    
    
    /**
     * constructor of the factory
     */
    public PostgresqlDBConnectionFactory() {
        super();
    }
    
    /**
     * return the DBConnection object for Alignment uses
     */
    @Override
    public DBConnectionAlignment getDBConnectionAlignment() {
        return new PostgresqlDBConnection(configuration);

    }

    /**
     * return the DBconnection object for the Indexer
     * @return
     */
    @Override
    public DBConnectionIndexerInterface getDBConnectionIndexer() {
        return null;        
    //    return new OrientDBConnection(configuration);
    }

    /**
     * return the DBconnection object used for Testing
     */
    @Override
    public DBConnectionTestInterface getDBConnectionTestInterface() {
        System.out.println("config = " + configuration);
        return new PostgresqlDBConnection(configuration);
    }

}
