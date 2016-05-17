package gov.pnnl.stucco.dbconnect.postgresql;

import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConnectionBase;
import gov.pnnl.stucco.dbconnect.DBConstraint;
import gov.pnnl.stucco.dbconnect.StuccoDBException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet; 
import java.sql.SQLException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import org.json.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresqlDBConnection extends DBConnectionBase {
    private static final String POSTGRESQL_TABLES = "postgresql/tables.json";
    private static final String[] SELECT_FROM_TABLE;
    private static final JSONObject tables;
    private static Logger logger;
    private Map<String, Object> configuration;
    private Connection connection;
    private Statement statement;

    static {
        try {
            tables = new JSONObject(IOUtils.toString(PostgresqlDBConnection.class.getClassLoader().getResourceAsStream(POSTGRESQL_TABLES), "UTF-8"));
    /*        JSONObject newTables = new JSONObject();
            for (Object k : tables.keySet()) {
                JSONObject table = tables.getJSONObject(k.toString());
                JSONObject newTable = new JSONObject();
                for (Object i : table.keySet()) {
                    JSONObject json = new JSONObject();
                    String value = table.getString(i.toString());
                    String type = table.getString(i.toString()).split(" ")[0];
                    int index = table.getString(i.toString()).indexOf(" ");
                    int start = index + 1;
                    int end = value.length() - 1;
                    json.put("type", type);
                    json.put("constraint", value);
                    newTable.put(i.toString(), json);
                }
                newTables.put(k.toString(), newTable);
            }
            System.out.println(newTables.toString(2));
    */  
            Set<String> set = tables.keySet();
            set.remove("Edges");
            SELECT_FROM_TABLE = set.toArray(new String[0]);
            for (int i = 0; i < SELECT_FROM_TABLE.length; i++) {
                StringBuilder sb = new StringBuilder()
                    .append("SELECT * FROM ")
                    .append(SELECT_FROM_TABLE[i]);
                SELECT_FROM_TABLE[i] = sb.toString();
            }
        } catch (IOException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("error occured during tables.json file loading for PostgreSQL");
        } 
    }
    
    public PostgresqlDBConnection(Map<String, Object> configuration) {
        this.configuration = new HashMap<String, Object>();
        this.configuration.putAll(configuration);
        logger = LoggerFactory.getLogger(PostgresqlDBConnection.class);
        //vertIDCache = new HashMap<String, String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
        //vertIDCacheRecentlyRead = new HashSet<String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
    }

    /**
     * Open the DB prior to working with the system, for every open there should be a 
     * corresponding close()
     * If the system is already open() not other connections will be made from this thread
     */
    @Override
    public void open() {
        try { 
            //PostgreSQL connecting url has following form: jdbc:postgresql://hostname:port/database
            StringBuilder sb = new StringBuilder();
            sb.append("jdbc:postgresql://");
            sb.append(configuration.get("hostname").toString());
            sb.append(":");
            sb.append(configuration.get("port").toString());
            sb.append("/");
            sb.append(configuration.get("database").toString());
            String url = sb.toString();
        //    String username = configuration.get("username").toString();
        //    String password = configuration.get("password").toString();
            connection = DriverManager.getConnection(url, null, null);
            statement = connection.createStatement();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("could not create PostgreSQL client connection");
        }

        try {
            createTables();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("could not create PostgreSQL tables");
        } 
    };

    private static String getStackTrace(Exception e){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    private void createTables() throws SQLException {
        statement.executeUpdate("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";");
        for (Object key : tables.keySet()) {
            String tableName = key.toString();
            JSONObject table = tables.getJSONObject(tableName);
            String sql = buildCreateTableSQL(tableName, table);
            statement.executeUpdate(sql);
        } 
    }

    private String buildCreateTableSQL(String tableName, JSONObject table) {
        String prefix = "";
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sql.append(tableName);
        sql.append(" (");
        for (Object key : table.keySet()) {
            sql.append(prefix);
            String propertyName = key.toString();
            String constraint = table.getJSONObject(propertyName).getString("constraint");
            sql.append(propertyName);
            sql.append(" ");
            sql.append(constraint);
            prefix = ", ";
        }
        sql.append(");");
        
        return sql.toString();
    }

    /**
     * Close the DB and commit any transactions, for certain system it may be a NO-OP
     */
    @Override
    public void close() {
        try {
        //    statement.executeUpdate("DROP TABLE addressrange, campaign, course_of_action, exploit, exploit_target, incident, indicator, ip, observable, threat_actor, ttp, vulnerability, weakness, malware;");
            connection.close();
            statement.close();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("could not close PostgreSQL client connection");
        }
    };

    /**
     * Builds the index for a set of properties per their specification
     * @param indexConfig
     * @throws StuccoDBException 
     */
    @Override
    public void buildIndex(String indexConfig) throws StuccoDBException {};


    /**
     * Given a property map add a new vertex to the DB
     * @param properties - the property map of the vertex
     * @return ID of the vertex as defined in the DB
     */
    @Override
    public String addVertex(Map<String, Object> properties) {
        if (!properties.containsKey("name") || !properties.containsKey("vertexType")) {
            String msg = String.format("cannot add vertex with missing or invalid vertex name or vertexType");
            throw new IllegalArgumentException(msg);
        }
        String id = null;
        try {
            String sql = buildInsertSQL(properties.get("vertexType").toString(), properties);
            statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            ResultSet rs = statement.getGeneratedKeys();
            rs.next();
            id = rs.getString("id");
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to add new vertex with name - " + properties.get("name") + ", and vertexType - " + properties.get("vertexType"));
        }    

        return id;
    };

    /**
     * build add (insert into table) vertex sql 
     * @param properties - vertex properties to be converted to sql string
     * @return sql string 
     */
    private static String buildInsertSQL(String tableName, Map<String, Object> properties) {
        String prefix = "";
        JSONObject table = tables.getJSONObject(tableName);
        StringBuilder propertiesSQL = new StringBuilder();
        StringBuilder valuesSQL = new StringBuilder();
        for (String propertyName : properties.keySet()) {
            propertiesSQL.append(prefix);
            valuesSQL.append(prefix);
            propertiesSQL.append(propertyName);
            JSONObject propertyConstraint = table.getJSONObject(propertyName);
            String value;
            switch (propertyConstraint.getString("type")) {
                case "array":
                    List<String> set = (List<String>) properties.get(propertyName);
                    value = buildArrayString(set);
                    valuesSQL.append(value);
                    break;
                case "bigint":
                    value = properties.get(propertyName).toString();
                    valuesSQL.append(value);
                    break;
                default:
                    value = properties.get(propertyName).toString();
                    valuesSQL.append("'");
                    valuesSQL.append(value);
                    valuesSQL.append("'");
                    break;

            }
            prefix = ", ";
        }

        StringBuilder sql = new StringBuilder("INSERT INTO ")
            .append(tableName)
            .append(" (")
            .append(propertiesSQL)
            .append(") ")
            .append("VALUES (")
            .append(valuesSQL)
            .append(");");

        return sql.toString();
    }

    /**
     * turn list into string of following form (including double quotes): "{'value1', 'value2'}";
     * NOTE: function treats all list values as strings for now
     * @param list - list of values to be inserted into table
     * @return string representation of list values acceptable by sql
     */
    private static String buildArrayString(List<String> list) {
        String prefix = "";
        StringBuilder sb = new StringBuilder("'{");
        for (String value : list) {
            sb.append(prefix);
            sb.append("\"");
            sb.append(value);
            sb.append("\"");
            prefix = ", ";
        }
        sb.append("}'");

        return sb.toString();
    }

    /**
     * Retrieves the vertex's property map as referenced by the vertex ID
     * @param id as per the DB
     * @return a property map of the vertex, user must know based on the key how to recast the object type to use its value
     */
    @Override
    public Map<String, Object> getVertByID(String id) {
        System.out.println("In get vert by id = " + id);
        if (id == null || id.isEmpty()) {
            return null;
        }
        for (String select_from_table : SELECT_FROM_TABLE) {
            StringBuilder sql = new StringBuilder(select_from_table)
                .append(" WHERE ID = '")
                .append(id)
                .append("';");
            try {
                ResultSet rs = statement.executeQuery(sql.toString());
                rs.next();
                System.out.println(rs);
            } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
            }
        }

        return null; 
    };

    /**
     * create an edge given two vertex IDs with a specific relationship type
     * @param inVertID - in vertex ID
     * @param outVertID - out vertex ID
     * @param relation - relationship type
     */
    @Override
    public void addEdge(String inVertID, String outVertID, String relation) {
        if (relation == null || relation.equals("") ) {
            throw new IllegalArgumentException("cannot add edge with missing or invalid relation");
        }
        if (inVertID == null || inVertID.equals("") ) {
            throw new IllegalArgumentException("cannot add edge with missing or invalid inVertID");
        }
        if (outVertID == null || outVertID.equals("")) {
            throw new IllegalArgumentException("cannot add edge with missing or invalid outVertID");
        } 
       
        StringBuilder sql = new StringBuilder()
            .append("INSERT INTO Edges (inVertID, outVertID, relation) VALUES ('")
            .append(inVertID)
            .append("', '")
            .append(outVertID)
            .append("', '")
            .append(relation)
            .append("');");
        try {
            statement.executeUpdate(sql.toString());
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to add new edge with outVertID=" + outVertID + ", inVertID=" + inVertID + ", relation=" + relation);
        }    
    };

    /**
     * returns list of edge info maps for the outgoing edges of this vertex
     * @param vertName
     * @return list of edge property maps
     */
    @Override
    public List<Map<String, Object>> getOutEdges(String outVertID){
        return null;
    };

    /**
     * returns list of edge info maps for the incoming edges of this vertex
     * @param vertName
     * @return list of edge property maps
     */
    @Override
    public List<Map<String, Object>> getInEdges(String inVertID){
        return null;
    };

    /**
     * Identify the vertices where their relationship type and direction enter the specified vertex
     * @param v1 - vertex end point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    @Override
    public List<String>getInVertIDsByRelation(String v1, String relation){
        return null;
    };

    /**
     * Identify the vertices where their relationship type and direction leave the specified vertex
     * @param v1 - vertex starting point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    @Override
    public List<String>getOutVertIDsByRelation(String v1, String relation){
        return null;
    };

    /**
     * Identify all vertices where their relationship type and direction either enter or leave the specified vertex
     * @param v1 - vertex starting or ending point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    @Override
    public List<String>getVertIDsByRelation(String v1, String relation){
        return null;
    };
    
    /**
     * Identify the vertices where their relationship type and direction enter the specified vertex
     * and where the found vertices match the following constraints
     * @param v1 - vertex ending point
     * @param relation - the relationship type of the edge
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    @Override
    public List<String> getInVertIDsByRelation(String v1, String relation, List<DBConstraint> constraints){
        return null;
    };
    
    /**
     * Identify the vertices where their relationship type and direction leave the specified vertex
     * and where the found vertices match the following constraints
     * @param v1 - vertex starting point
     * @param relation - the relationship type of the edge
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    @Override
    public List<String> getOutVertIDsByRelation(String v1, String relation, List<DBConstraint> constraints){
        return null;
    };

    /**
     * Identify all vertices where their relationship type and direction either enter or leave the specified vertex
     * and where the found vertices match the following constraints
     * @param v1 - vertex starting or ending point
     * @param relation - the relationship type of the edge
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    @Override
    public List<String> getVertIDsByRelation(String v1, String relation, List<DBConstraint> constraints){
        return null;
    };
    
    /**
     * Perform a query/search of the DB using the following constraints on the request
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    @Override
    public List<String> getVertIDsByConstraints(List<DBConstraint> constraints){
        return null;
    };
    
    /**
     * Given two vertices and a relation, remove the edge
     * @param inVertID - in vertex ID
     * @param outVertID - out vertex ID
     * @param relation - relationship type
     */
    @Override
    public void removeEdgeByRelation(String inVertID, String outVertID, String relation){};

    /**
     * Given a specific vertex remove it from the DB
     * (Note any edges connected to it will also be removed)
     * @param id - vertex ID
     */
    @Override
    public void removeVertByID(String id){};
    
    /**
     * Replaces current vertex's property map with a different one
     * @param id - vertex that will be changed
     * @param properties - property map with different contents, complete replacement of current content
     */
    @Override
    public void updateVertex(String id, Map<String, Object> properties){};

    @Override
    protected void setPropertyInDB(String id, String key, Object newValue) {

    }

    /**
     * remove all vertices in the databases
     */
    @Override
    public void removeAllVertices() {
        for (Object tableName : tables.keySet()) {
            String sql = String.format("DELETE FROM %s", tableName.toString());
            executeSQLQuery(sql);
        }
        executeSQLQuery("DELETE FROM Edges");
    };

    /**
     *  helper function to execute sql queries when return is not reqired
     */
    private void executeSQLQuery(String query) {
        try {
            statement.executeUpdate(query);
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to execute query: " + query);
        }
    }

    /**
     * load db state from the specified file
     */
    @Override
    public void loadState(String filePath){};

    /**
     * save db state to the specified file
     */
    @Override
    public void saveState(String filePath){};

        /**
     * gets the number of vertices in the graph
     */
    @Override
    public long getVertCount() {
        long l = 1L;
        return l;
    };
    
    /**
     * gets the number of edges in the graph
     */
    @Override
    public long getEdgeCount(){
        return 1L;
    };

    /**
     * get the number of edges based on incoming vertex and outgoing vertex and a relationship
     * @param inVertID - incoming vertex
     * @param outVertID - outgoing vertex
     * @param relation - label/relations on the edge
     * @return a count of edges
     */
    @Override
    public int getEdgeCountByRelation(String inVertID, String outVertID, String relation){
        return 0;
    };
    
    /**
     * 
     * @param property name
     * @param condition 
     * @param value
     */
    @Override
    public DBConstraint getConstraint(String property, Condition condition, Object value){
        return null;
    };
}
