package gov.pnnl.stucco.dbconnect.orientdb;

import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionIndexerInterface;
import gov.pnnl.stucco.dbconnect.orientdb.CommandLine.UsageException;
import gov.pnnl.stucco.dbconnect.orientdb.OrientDBConnection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;


/**
 * Parses the Stucco index specification, in order to set up indexes. The index 
 * specification format is assumed to be the following JSON:
 * 
 * <pre><block>
 * 
 * "indexes": [ 
 *   {
 *     type: ("NOTUNIQUE"|"FULLTEXT")
 *     keys: [
 *       {
 *         "name": propertyName
 *         "class": ("String"|"Character"|"Boolean"|"Byte"|"Short"|"Integer"|"Long"|"Float"|"Double"|"Decimal"|"Precision"|"Geoshape")
 *       }
 *       ...
 *     ]
 *   }
 *   ...
 * ]
 *         
 * </block></pre>
 */
public class IndexDefinitionsToOrientDB {
    private static final Logger logger = LoggerFactory.getLogger(IndexDefinitionsToOrientDB.class);

    // Keys within the index specification JSON
    private static final String INDEXES = "indexes";
    private static final String TYPE = "type";
    private static final String KEYS = "keys";
    private static final String NAME = "name";
    private static final String CLASS = "class";
    
    // Other String constants
    private static final String STRING = "String";

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

    
    public IndexDefinitionsToOrientDB() {
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
        
        String type = indexSpec.getString(TYPE);
        JSONArray keys = indexSpec.getJSONArray(KEYS);
        
        List<String> propertyNames = new ArrayList<String>();
        String propertyDeclarations = buildPropertyDeclarations(keys, propertyNames);
        request.append(propertyDeclarations);
        this.propertyNames.addAll(propertyNames); // a global list of all property Names
        
        buildIndexDeclaration(type, propertyNames);
    }

    /**
     * Builds OrientDB declaration for the properties used in an index.
     * 
     * @param keys           The keys in JSON form
     * @param propertyNames  (OUT) The names of the keys, returned by side effect
     * 
     * @return OrientDB declaration of properties
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
            
            try {
                // Make the property declaration for the key
                String declaration = buildPropertyDeclaration(property, classType);
                ((OrientDBConnection)dbConnection).<OrientDynaElementIterable>executeSQL(declaration);
                ((OrientDBConnection)dbConnection).commit();
                declarations.append(declaration);
            } catch (OCommandExecutionException e) {
                // Intercept error to print debug info
                String stackTrace = getStackTrace(e);
                logger.error(String.format("Failed to generate property '%s'", property));
                logger.error(stackTrace);
                
                // Re-throw up the stack
                throw e;
            }
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
     */
    private String buildPropertyDeclaration(String propertyName, String classType) {
        // Normalize to forms needed in request
        propertyName = propertyName.trim();
        classType = capitalize(classType.trim());

        // Build the declaration
        String declaration = String.format("CREATE PROPERTY V.%s %s", propertyName, classType);

        return declaration;
    }
    
    /** Builds a DB index declaration. */
    private void buildIndexDeclaration(String indexType, List<String> propertyKeys) {
        
        for(String key : propertyKeys) {
            try {
                // Make up a name
                String indexName = generateIndexName();        
                String declaration = String.format("CREATE INDEX %s ON V (%s) %s", indexName, key, indexType);
                ((OrientDBConnection)dbConnection).<Integer>executeSQL(declaration);
                ((OrientDBConnection)dbConnection).commit();
            } catch (OCommandExecutionException e) {
                // Intercept error so we can report it
                String stackTrace = getStackTrace(e);
                logger.error(String.format("Failed to create index for '%s'", key));
                logger.error(stackTrace); 
                
                // Re-throw up the stack
                throw e;
            }
        }
        
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
        
        // Handle each index
        int count = indexes.length();
        for (int i = 0; i < count; i++) {
            JSONObject indexSpec = indexes.getJSONObject(i);
            JSONArray keys = indexSpec.getJSONArray(KEYS);
            
            // For each key
            int keyCount = keys.length();
            for (int j = 0; j < keyCount; j++) {
                JSONObject keySpec = keys.getJSONObject(j);
                
                // Get the property name
                String property = keySpec.getString(NAME);
                this.propertyNames.add(property); // tracking the global list of property names
                
                try {
                    // Make the property declaration for the key
                    String declaration = String.format("DROP PROPERTY V.%s FORCE", property);
                    ((OrientDBConnection)dbConnection).<OrientDynaElementIterable>executeSQL(declaration);
                } catch (OCommandExecutionException e) {
                    // Intercept error to print debug info
                    String stackTrace = getStackTrace(e);
                    logger.error(String.format("Failed to drop property '%s'", property));
                    logger.error(stackTrace);
                    
                    // Re-throw up the stack
                    throw e;
                } catch (OSchemaException e) {
                    logger.info(String.format("IGNORING: No property with name: '%s'", property));
                    
                }
            }
        }
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
    public Set<String> getPropertNamesFromDB() {
        Set<String> dbPropertyNames = new HashSet<String>();

        String query = "SELECT field FROM (SELECT expand(indexDefinition) FROM (SELECT expand(indexes) FROM metadata:indexmanager))";
        try {
            OrientDynaElementIterable qiterable=((OrientDBConnection)dbConnection).<OrientDynaElementIterable>executeSQL(query);
            if (qiterable != null) { // Don't know if this can happen, but just in case
                Iterator<Object> iter = qiterable.iterator();
                while (iter.hasNext()) {
                    OrientVertex v = (OrientVertex) iter.next();
                    String fieldName = (String)v.getProperty("field");
                    dbPropertyNames.add(fieldName);
                }
            }
        } catch (OCommandExecutionException e) {
            // Intercept error to print debug info
            e.printStackTrace();
            // Re-throw up the stack
            throw e;
        }
        return dbPropertyNames;
    }
    
    public static void main(String[] args) {
        try {
            
            IndexDefinitionsToOrientDB loader = new IndexDefinitionsToOrientDB();
            
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
        catch (OCommandExecutionException e) {
            System.err.printf("Indexing failed");
            System.exit(-1);
        }
    }



}
