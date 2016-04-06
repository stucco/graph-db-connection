package gov.pnnl.stucco.dbconnect;


public class IndexDefinitionsToDB {

    public IndexDefinitionsToDB() {
        // TODO Auto-generated constructor stub
    }
    
    public static void main(String[] args) {
        try {
            
            // get environment variables
            String type = System.getenv("STUCCO_DB_TYPE");
            if (type == null) {
                throw (new NullPointerException("Missing environment variable STUCCO_DB_TYPE"));
            }

            DBConnectionFactory factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.valueOf(type));

            String config = System.getenv("STUCCO_DB_CONFIG");
            if (config == null) {
                throw (new NullPointerException("Missing environment variable STUCCO_DB_CONFIG"));
            }
            factory.setConfiguration(config);
            
            String indexConfig = System.getenv("STUCCO_DB_INDEX_CONFIG");
            if (indexConfig == null) {
                throw (new NullPointerException("Missing environment variable STUCCO_DB_INDEX_CONFIG"));
            }
            
            DBConnectionIndexerInterface conn = factory.getDBConnectionIndexer();
            conn.open();
            
            conn.buildIndex(indexConfig);
        } 
        catch (StuccoDBException e)
        {
            System.err.printf("Error in creating index: %s\n", e.toString());
            System.exit(-1);
        }
    }
}
