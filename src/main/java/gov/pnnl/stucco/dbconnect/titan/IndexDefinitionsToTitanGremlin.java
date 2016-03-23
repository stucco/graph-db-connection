package gov.pnnl.stucco.dbconnect.titan;

import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionIndexerInterface;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tinkerpop.rexster.client.RexProException;


//BELOW FROM ORIGINAL TITAN GREMLIN 
/**
 * Parses the Stucco index specification, in order to issue Gremlin requests for
 * setting up indexes. The index specification format is assumed to be the 
 * following JSON:
 * 
 * <pre><block>
 * 
 * "indexes": [ 
 *   {
 *     type: ("composite"|"mixed")
 *     keys: [
 *       {
 *         "name": propertyName
 *         "class": ("String"|"Character"|"Boolean"|"Byte"|"Short"|"Integer"|"Long"|"Float"|"Double"|"Decimal"|"Precision"|"Geoshape")
 *         "cardinality": ("SINGLE"|"LIST"|"SET")
 *       }
 *       ...
 *     ]
 *   }
 *   ...
 * ]
 *         
 * </block></pre>
 */

public class IndexDefinitionsToTitanGremlin {
    private static final Logger logger = LoggerFactory.getLogger(IndexDefinitionsToTitanGremlin.class);

    // Keys within the index specification JSON
    private static final String INDEXES = "indexes";
    private static final String TYPE = "type";
    private static final String KEYS = "keys";
    private static final String NAME = "name";
    private static final String CLASS = "class";
    private static final String CARDINALITY = "cardinality";
    
    // Other String constants
    private static final String STRING = "String";
    private static final String COMPOSITE = "composite";
    private static final String MIXED = "mixed";

    /** Number of seconds to pause after initiating connection. */
    private static final int WAIT_TIME = 1;
    
    /** Our gateway to the DB. */
    private DBConnectionIndexerInterface dbConnection;
    
    /** 
     * If true, use the test config for the DBConnection; otherwise
     * use the default config.
     */
    private boolean testMode;

    /** Counter used to generate unique index names. */
    private int indexNumber = 0;
    
    /** debug content for unit test */
    private Set<String> propertyNames = new HashSet<String>();

    
    public IndexDefinitionsToTitanGremlin() {
        // Instantiable but only from this file    
    }
    
    private void setTestMode(boolean flag) {
        testMode = flag;
    }
    
    /**
     * specifies the orientDB dbconnection 
     * @param dbConnection
     */
    public void setDBConnection(DBConnectionIndexerInterface dbConnection) {
        this.dbConnection = dbConnection;
    }
    
    /** 
     * Loads the index specification file, and parses it in order to prepare for
     * graph loading. The preparation includes declaration of indexes and of any
     * properties used in the indexes.
     */
    public void parse(File file) throws IOException {
        
        // Load the schema file into a JSONObject
        String str = getTextFileContent(file);
        JSONObject root = new JSONObject(str);
        
        // Get the index entries from the JSON
        JSONArray indexes = root.optJSONArray(INDEXES);
        if (indexes == null) {
            logger.error("Expected 'indexes' key");
        }
        
        // Handle each index
        int count = indexes.length();
        for (int i = 0; i < count; i++) {
            JSONObject indexSpec = indexes.getJSONObject(i);
            parseIndexSpec(indexSpec);
        }
    }

    /**
     * Parses a single index specification, declaring it, and any properties it
     * uses, to DB. 
     */
    private void parseIndexSpec(JSONObject indexSpec) {
        StringBuilder request = new StringBuilder();
        request.append("m = g.getManagementSystem();");
        
        String type = indexSpec.getString(TYPE);
        JSONArray keys = indexSpec.getJSONArray(KEYS);
        
        List<String> propertyNames = new ArrayList<String>();
        String propertyDeclarations = buildPropertyDeclarations(keys, propertyNames);       
        request.append(propertyDeclarations);
        
        String indexDeclaration = buildIndexDeclaration(type, propertyNames);
        if (indexDeclaration.isEmpty()) {
            // Couldn't create it
            logger.error(String.format("Couldn't create index (type = %s)", type));
            return;
        }
        
        request.append(indexDeclaration);
        
        request.append("m.commit();");
        executeRexsterRequest(request.toString());
    }

    /**
     * Builds Gremlin declaration for the properties used in an index.
     * 
     * @param keys           The keys in JSON form
     * @param propertyNames  (OUT) The names of the keys, returned by side effect
     * 
     * @return Gremlin declaration of properties
     */
    private String buildPropertyDeclarations(JSONArray keys, List<String> propertyNames) {
        StringBuilder declarations = new StringBuilder();
        
        // For each key
        int keyCount = keys.length();
        for (int i = 0; i < keyCount; i++) {
            JSONObject keySpec = keys.getJSONObject(i);
            
            // Get the property name
            String property = keySpec.getString(NAME);
            propertyNames.add(property);
            
            // Get the class type, defaulting to String
            String classType = keySpec.optString(CLASS);
            if (classType == null) {
                classType = STRING;
            }
            
            // Get the cardinality, defaulting to SINGLE
            String cardinality = keySpec.optString(CARDINALITY);
            if (cardinality == null) {
                cardinality = "";
            }

            // Make the property declaration for the key
            String declaration = buildPropertyDeclaration(property, classType, cardinality);
            declarations.append(declaration);
        }
        
        return declarations.toString();
    }
    
    /** Converts a Throwable to a String. */
    private String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        String str = sw.toString(); 
        return str;    
    }
    
    /** 
     * Builds the declaration of a new property. 
     * 
     * @param propertyName  Name of property
     * @param classType     Class type of property
     * @param cardinality   Cardinality of property ("" means don't declare) 
     */
    private String buildPropertyDeclaration(String propertyName, String classType, String cardinality) {
        // Normalize to forms needed in request
        propertyName = propertyName.trim();
        classType = capitalize(classType.trim());
        cardinality = cardinality.trim().toUpperCase();

        // Build the declaration
        StringBuilder declaration = new StringBuilder();
        declaration.append(String.format("m.makePropertyKey('%s')", propertyName));
        declaration.append(String.format(".dataType(%s.class)", classType));
        if (!cardinality.isEmpty()) {
            declaration.append(String.format(".cardinality(Cardinality.%s)", cardinality));
        }
        declaration.append(".make();");

        return declaration.toString();
    }
    
    /** Builds a Gremlin index declaration. */
    private String buildIndexDeclaration(String indexType, List<String> propertyKeys) {
        StringBuilder declaration = new StringBuilder();
        
        // Make up a name
        String indexName = generateIndexName();
        declaration.append(String.format("m.buildIndex('%s', Vertex.class)", indexName));
 
        
        String keysDeclaration = buildAddKeysDeclaration(propertyKeys);
        if (keysDeclaration.isEmpty()) {
            // Couldn't because one or more keys was already used
            return "";
        }
        
        declaration.append(keysDeclaration);
        
        if (indexType.equalsIgnoreCase(COMPOSITE)) {
            declaration.append(".buildCompositeIndex();");
        }
        else if (indexType.equalsIgnoreCase(MIXED)) {
            declaration.append(".buildMixedIndex('search');");
        }
        else {
            // Not a recognized type
            logger.error("Unrecognized index type: " + indexType);
            return "";
        }
        
        return declaration.toString();
    }
    
    /** 
     * Builds the partial Gremlin declaration for adding property keys to an
     * index.
     */
    private String buildAddKeysDeclaration(List<String> propertyKeys) {
        StringBuilder declaration = new StringBuilder();
        for (String key : propertyKeys) {
            declaration.append(String.format(".addKey(m.getPropertyKey('%s'))", key));
        }
        
        return declaration.toString();
    }

    /** Auto-generates an index name. */
    private String generateIndexName() {
        return "index" + indexNumber++;
    }
    
    /** Converts a String to first character uppercase, remainder lowercase. */
    private static String capitalize(String str) {
        if (str.isEmpty()) {
            return str;
        }
        
        String first = str.substring(0, 1);
        String rest = str.substring(1);
        String capitalized = first.toUpperCase() + rest.toLowerCase();
        return capitalized;
    }
    
    /** Gets a text file's content as a String. */
    private String getTextFileContent(File textFile) throws IOException {
        try (BufferedReader in = new BufferedReader(new FileReader(textFile))) {
            StringBuilder builder = new StringBuilder();
            String eol = System.getProperty("line.separator");
            String line;
            while ((line = in.readLine()) != null) {
                builder.append(line);
                builder.append(eol);
            }
            
            String str = builder.toString();
            return str;
        }
    }
    /**
     * removes all indexes previously created and uses the information in the file to know which ones
     * to remove.  It will only remove the properties specified in the file
     * @param file
     * @throws IOException 
     */
    public void dropAllIndexes(File file) throws IOException {
        // Load the schema file into a JSONObject
        String str = getTextFileContent(file);
        JSONObject root = new JSONObject(str);
        
        // Get the index entries from the JSON
        JSONArray indexes = root.optJSONArray(INDEXES);
        if (indexes == null) {
            logger.error("Expected 'indexes' key");
        }
        
        //TODO: rewrite this for TITAN

    }
    
    /**
     * return the property names as found in the config file
     * @return
     */
    public Set<String> getPropertyNames() {
        return this.propertyNames;
    }
    
    /**
     * identify the names of the properties that were made specific to the indexes
     * @return
     */
    public Set<String> getPropertyNamesFromDB() {
        Set<String> dbPropertyNames = new HashSet<String>();
        //TODO:  NEED TO WRITE THIS

        return dbPropertyNames;
    }
    
    /** Sends a request to Rexster. */
    private void executeRexsterRequest(String request) {
        logger.info("Making Rexster request: " + request);
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                ((TitanDBConnection)dbConnection).executeGremlin(request);
                logger.info("    Rexster request succeeded");
                return;
            } 
            catch (RexProException | IOException e) {
                logger.error("    Rexster request failed: " + e); 
            }
        }
        logger.error("    Skipping Rexster request after 3 failed attempts.");
    }
    
    public static void main(String[] args) {
        try {
            
            IndexDefinitionsToTitanGremlin loader = new IndexDefinitionsToTitanGremlin();
            
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
            
            loader.setDBConnection(conn);
            
            // load the schema and create the indexes
            loader.dropAllIndexes(new File (indexConfig));
            loader.parse(new File(indexConfig));
        } 
        catch (IOException e) {
            System.err.printf("Error in opening file, path or file does not exist: %s\n", e.toString());
            System.exit(-1);
        }
    }

}
