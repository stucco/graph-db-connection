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
import java.sql.Array;
import java.sql.ResultSetMetaData;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet; 
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.IOUtils;

import org.json.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresqlDBConnection extends DBConnectionBase {
    private static final String POSTGRESQL_TABLES = "postgresql/tables.json";
    private static final String[] SELECT_FROM_TABLE;
    private static final JSONObject vertTables;
    private static final JSONObject edgeTable;
    private static Logger logger;
    private Map<String, Object> configuration;
    private Connection connection;
    private Statement statement;

    static {
        try {
            JSONObject tables = new JSONObject(IOUtils.toString(PostgresqlDBConnection.class.getClassLoader().getResourceAsStream(POSTGRESQL_TABLES), "UTF-8"));
            vertTables = tables.getJSONObject("vertices");
            edgeTable = tables.getJSONObject("edges");
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
        //creating tables for vertices
        for (Object key : vertTables.keySet()) {
            String tableName = key.toString();
            JSONObject table = vertTables.getJSONObject(tableName);
            String sql = buildCreateTableSQL(tableName, table);
            statement.executeUpdate(sql);
        } 
        //creating table for edges
        String sql = ("CREATE TABLE IF NOT EXISTS Edges (outVertID uuid NOT NULL, inVertID uuid NOT NULL, relation text NOT NULL);");
        statement.executeUpdate(sql);
    }

    private String buildCreateTableSQL(String tableName, JSONObject table) {
        String prefix = "";
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sql.append(tableName);
        sql.append(" (_id uuid PRIMARY KEY DEFAULT uuid_generate_v4() NOT NULL UNIQUE, ");
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
            id = rs.getString("_id");
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
        JSONObject table = vertTables.getJSONObject(tableName);
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
        if (id == null || id.isEmpty()) {
            return null;
        }
        Map<String, Object> vertex = null;
        for (Object key : vertTables.keySet()) {
            String tableName = key.toString();
            StringBuilder sql = new StringBuilder("SELECT * FROM ")
                .append(tableName)
                .append(" WHERE _id = '")
                .append(id)
                .append("';");
            try {
                ResultSet rs = statement.executeQuery(sql.toString());
                if (rs.next()) {
                    vertex = convertResultSetToMap(tableName, rs);
                    break;
                }
            } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
            }
        }

        for (String n : vertex.keySet()) {
            System.out.println(n + " = " + vertex.get(n));
        }

        return vertex; 
    };

    /**
     * take ResultSet from sql query and converts it into HashMap<String, Object>
     * @param rs - ResultSet containing all sql selected values
     * @param tableName - name of a table that was quered, required to check if value is an array
     * @return map - map with key = column name and value = column value
     */
    private Map<String, Object> convertResultSetToMap(String tableName, ResultSet rs) throws SQLException {
        Map<String, Object> map = new HashMap<String, Object>();
        JSONObject table = vertTables.getJSONObject(tableName);
        for (Object column : table.keySet()) {
            String columnName = column.toString();
            String valueType = table.getJSONObject(columnName).getString("type");
            if (valueType.equals("array")) {
                Array array = rs.getArray(columnName);
                if (array != null) {
                    Object value = Arrays.asList((Object[])array.getArray());
                    map.put(columnName, value);
                }
            } else {
                Object value = rs.getObject(columnName);
                if (value != null) {
                    map.put(columnName, value);
                }
            }
        }

        return map;
    }

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
    public List<Map<String, Object>> getOutEdges(String outVertID) {
        if (outVertID == null || outVertID.equals("")) {
            throw new IllegalArgumentException("cannot find edges with missing or invalid outVertID");
        } 
        List<Map<String, Object>> outEdges = new ArrayList<Map<String, Object>>();
        StringBuilder query = new StringBuilder()
            .append("SELECT * FROM Edges WHERE outVertID = '")
            .append(outVertID)
            .append("';");
        try {
            ResultSet rs = statement.executeQuery(query.toString());
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                outEdges.add(map);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to select out edges for outVertID = " + outVertID);
        }    

        return outEdges;
    };

    /**
     * returns list of edge info maps for the incoming edges of this vertex
     * @param vertName
     * @return list of edge property maps
     */
    @Override
    public List<Map<String, Object>> getInEdges(String inVertID) {
        if (inVertID == null || inVertID.equals("")) {
            throw new IllegalArgumentException("cannot find edges with missing or invalid inVertID");
        } 

        List<Map<String, Object>> inEdges = new ArrayList<Map<String, Object>>();
        StringBuilder query = new StringBuilder()
            .append("SELECT * FROM Edges WHERE inVertID = '")
            .append(inVertID)
            .append("';");
        try {
            ResultSet rs = statement.executeQuery(query.toString());
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                inEdges.add(map);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to select in edges for inVertID = " + inVertID);
        }      

        return inEdges;
    };

    /**
     * convert ResultSet from quering Edges table to Map<String, Object>
     */
    private static Map<String, Object> edgesResultSetToMap(ResultSet rs) throws SQLException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("outVertID", rs.getString("outVertID"));
        map.put("inVertID", rs.getString("inVertID"));
        map.put("relation", rs.getString("relation"));

        return map;
    }

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
    public void removeEdgeByRelation(String inVertID, String outVertID, String relation) {
        if (relation == null || relation.equals("") ) {
            throw new IllegalArgumentException("cannot add edge with missing or invalid relation");
        }
        if (inVertID == null || inVertID.equals("") ) {
            throw new IllegalArgumentException("cannot add edge with missing or invalid inVertID");
        }
        if (outVertID == null || outVertID.equals("")) {
            throw new IllegalArgumentException("cannot add edge with missing or invalid outVertID");
        }
        StringBuilder query = new StringBuilder()
            .append("DELETE FROM Edges WHERE inVertID = '")
            .append(inVertID)
            .append("' AND outVertID = '")
            .append(outVertID)
            .append("' AND relation = '")
            .append(relation)
            .append("';");
        executeSQLQuery(query.toString());
    };

    /**
     * Given a specific vertex remove it from the DB
     * (Note any edges connected to it will also be removed)
     * @param id - vertex ID
     */
    @Override
    public void removeVertByID(String id) {
        if (id == null || id.equals("")) {
            throw new IllegalArgumentException("cannot find vert with missing or invalid id");
        } 
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            StringBuilder query = new StringBuilder()
                .append("DELETE FROM ")
                .append(tableName)
                .append(" WHERE _id = '")
                .append(id)
                .append("';");
            try {
                int result = statement.executeUpdate(query.toString());
                if (result == 1) {
                    break;
                }
            } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
                throw new StuccoDBException("failed to delete vertex with id = " + id);
            }
        }
        // removing corresponding edges
        removeEdgeByVertID(id);
    };

    /**
     * remove edge if it contains outVertID or inVertID matching id
     */
    private void removeEdgeByVertID(String id) {
        StringBuilder query = new StringBuilder()
            .append("DELETE FROM Edges WHERE outVertID = '")
            .append(id)
            .append("' OR inVertID = '")
            .append(id)
            .append("';");
        executeSQLQuery(query.toString());
    }
    
    /**
     * Replaces current vertex's property map with a different one
     * @param id - vertex that will be changed
     * @param properties - property map with different contents, complete replacement of current content
     */
    @Override
    public void updateVertex(String id, Map<String, Object> properties) {
    };

    @Override
    protected void setPropertyInDB(String id, String key, Object newValue) {

    }

    /**
     * remove all vertices in the databases
     */
    @Override
    public void removeAllVertices() {
        //removing content of vert tables 
        for (Object tableName : vertTables.keySet()) {
            String sql = String.format("DELETE FROM %s", tableName.toString());
            executeSQLQuery(sql);
        }
        //removing content of edge table
        executeSQLQuery("DELETE FROM Edges");
    };

    /**
     *  helper function to execute sql queries when return is not reqired
     */
    private void executeSQLQuery(String query) {
        try {
            statement.execute(query);
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
     * gets the number of vertices in the graph; aka number of rows in all vertex tables in the database
     */
    @Override
    public long getVertCount() {
        long count = 0L;
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            String countSQL = String.format("SELECT COUNT(*) FROM %s;", tableName);
            try {
                ResultSet rs = statement.executeQuery(countSQL);
                if (rs.next()) {
                    count += rs.getLong(1);
                }
            } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
                throw new StuccoDBException("failed to count rows in table: " + tableName);
            }
        }
        
        return count;
    };
    
    /**
     * gets the number of edges in the graph; aka number of rows in "Edges" table in the database
     */
    @Override
    public long getEdgeCount(){
        long count = 0L;
        try {
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM Edges;");
            if (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to count rows in Edge table");
        }

        return count;
    };

    /**
     * get the number of edges based on incoming vertex and outgoing vertex and a relationship
     * @param inVertID - incoming vertex
     * @param outVertID - outgoing vertex
     * @param relation - label/relations on the edge
     * @return a count of edges
     */
    @Override
    public int getEdgeCountByRelation(String inVertID, String outVertID, String relation) {
        if (relation == null || relation.equals("") ) {
            throw new IllegalArgumentException("cannot add edge with missing or invalid relation");
        }
        if (inVertID == null || inVertID.equals("") ) {
            throw new IllegalArgumentException("cannot add edge with missing or invalid inVertID");
        }
        if (outVertID == null || outVertID.equals("")) {
            throw new IllegalArgumentException("cannot add edge with missing or invalid outVertID");
        } 

        int count = 0;
        StringBuilder query = new StringBuilder()
            .append("SELECT COUNT(*) FROM Edges WHERE inVertID = '")
            .append(inVertID)
            .append("' AND outVertID = '")
            .append(outVertID)
            .append("' AND inVertID = '")
            .append(inVertID)
            .append("' AND relation = '")
            .append(relation)
            .append("';");
        try {
            ResultSet rs = statement.executeQuery(query.toString());
            if (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to count rows in Edge table");
        }

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
