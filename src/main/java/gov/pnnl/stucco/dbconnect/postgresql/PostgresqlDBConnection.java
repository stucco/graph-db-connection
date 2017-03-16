package gov.pnnl.stucco.dbconnect.postgresql;

import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConnectionBase;
import gov.pnnl.stucco.dbconnect.DBConstraint;
import gov.pnnl.stucco.dbconnect.StuccoDBException;
 
import gov.pnnl.stucco.dbconnect.postgresql.PostgresqlDBPreparedStatement;
import gov.pnnl.stucco.dbconnect.postgresql.PostgresqlDBPreparedStatement.Columns;
import gov.pnnl.stucco.dbconnect.postgresql.PostgresqlDBPreparedStatement.TYPE;
import gov.pnnl.stucco.dbconnect.postgresql.PostgresqlDBPreparedStatement.API;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement; 
import java.sql.PreparedStatement; 
import java.sql.ResultSet;    
import java.sql.SQLException;    
import java.sql.Array;
import java.sql.Timestamp;   
import java.sql.Types; 

import java.net.URISyntaxException;

import org.postgresql.copy.CopyManager; 
import org.postgresql.copy.CopyIn;
import org.postgresql.core.BaseConnection;
    
import java.io.PrintWriter;  
import java.io.StringWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream; 
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet; 
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays; 
import java.util.Collection;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.dbcp2.BasicDataSource;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresqlDBConnection extends DBConnectionBase {
    
    private static final String POSTGRESQL_TABLES = "postgresql/tables.json";
    private static final JSONObject vertTables;
    private static final JSONObject tables;
    private static Logger logger;
    private Map<String, Object> configuration;
    private Connection connection;
    private Statement statement;
    private PostgresqlDBPreparedStatement ps;
    private BasicDataSource connectionPool;

    private String hostname;
    private String port;
    private String dbName;
    private String url;
    private String username;
    private String password;

    static {
        try {
            tables = new JSONObject(IOUtils.toString(PostgresqlDBConnection.class.getClassLoader().getResourceAsStream(POSTGRESQL_TABLES), "UTF-8"));
            vertTables = tables.getJSONObject("vertices");
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

        hostname = configuration.get("hostname").toString();
        port = configuration.get("port").toString();
        dbName = configuration.get("database").toString();
        url = buildString("jdbc:postgresql://", hostname, ":", port, "/", dbName);
        username = (configuration.containsKey("username")) ? configuration.get("username").toString() : null;
        password = (configuration.containsKey("password")) ? configuration.get("password").toString() : null;
        
        initConnectionPool();
        initDB();
    }

    private void initConnectionPool() {
        connectionPool = new BasicDataSource();
        connectionPool.setUsername(username);
        connectionPool.setPassword(password);
        connectionPool.setDriverClassName("org.postgresql.Driver");
        connectionPool.setUrl(url);
        connectionPool.setInitialSize(5);
    }

    private void initDB() {
        open();
        PostgresqlDBInitialization initer = new PostgresqlDBInitialization(statement, tables);
        initer.initDB();
        close();
    }

    /**
     * Open the DB prior to working with the system, for every open there should be a 
     * corresponding close()
     * If the system is already open() not other connections will be made from this thread
     */ 
    @Override
    public void open() { 
        try {
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement(); 
            ps = new PostgresqlDBPreparedStatement(connection, vertTables);
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("could not create PostgreSQL client connection");
        }
    };

    /**
     * Close the DB and commit any transactions, for certain system it may be a NO-OP
     */
    @Override
    public void close() {
        try {
            connection.close();
            statement.close();
            ps.close();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("could not close PostgreSQL client connection");
        }
    };

    /**
     * Builds the index for full text search of sourceDocument
     * function implementation is in PostgresQLDBInit class
     * @param indexConfig - is not required for postgresql
     * @throws StuccoDBException 
     */
    @Override
    public void buildIndex(String filePath) throws StuccoDBException {};

    /**
     * Given a property map add a new vertex to the DB
     * @param properties - the property map of the vertex
     * @return ID of the vertex as defined in the DB
     */
    @Override
    public String addVertex(Map<String, Object> properties) {
        sanityCheck("add vertex", properties, "name", "vertexType");

        String id = null;
        try {
            String tableName = properties.get("vertexType").toString();
            PreparedStatement preparedStatement = ps.getPreparedStatement(tableName, API.ADD_VERTEX);
            JSONObject table = vertTables.getJSONObject(tableName).getJSONObject("columns");
            JSONArray order = vertTables.getJSONObject(tableName).getJSONArray("order");
            for (int i = 3; i < order.length(); i++) {
                String column = order.getString(i);
                int index = i - 2;
                switch (Columns.valueOf(column).type) {
                    case TEXT: 
                        if (properties.containsKey(column)) {
                            preparedStatement.setString(index, properties.get(column).toString());
                        } else {
                            preparedStatement.setNull(index, Types.VARCHAR);
                        }
                        break;
                    case ARRAY:
                        if (properties.containsKey(column)) {
                            Collection collection = (Collection) properties.get(column);
                            Object[] array = (Object[]) collection.toArray();
                            Array sqlArray = connection.createArrayOf("text", array);
                            preparedStatement.setArray(index, sqlArray);
                        } else {
                            preparedStatement.setNull(index, Types.ARRAY);
                        }
                        break;
                    case LONG:
                        if (properties.containsKey(column)) {
                            preparedStatement.setLong(index, ((Number)properties.get(column)).longValue());
                        } else {
                            preparedStatement.setNull(index, Types.BIGINT);
                        }
                        break;
                    case TIMESTAMP:
                        if (properties.containsKey(column)) {
                            preparedStatement.setTimestamp(index, (Timestamp)properties.get(column));
                        } else {
                            preparedStatement.setNull(index, Types.TIMESTAMP);
                        }
                        break;
                }
            }

            preparedStatement.executeUpdate();
            ResultSet rs = preparedStatement.getGeneratedKeys();
            if (rs.next()) {
                id = rs.getString("_id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("failed to add new vertex with name - " + properties.get("name") + ", and vertexType - " + properties.get("vertexType"));
        }    

        return id;
    };

    /**
     * Retrieves the vertex's property map as referenced by the vertex ID
     * @param id as per the DB
     * @return a property map of the vertex, user must know based on the key how to recast the object type to use its value
     */  
    @Override
    public Map<String, Object> getVertByID(String id) {
        sanityCheck("get vert", id, "id");

        Map<String, Object> vertex = null;
        try {

            Connection connection = connectionPool.getConnection();
            Statement poolStatement = connection.createStatement(); 
            for (Object key : vertTables.keySet()) {
                String tableName = key.toString();
                String query = String.format("SELECT * FROM %s WHERE _id ='%s';", tableName, id);

                ResultSet rs = poolStatement.executeQuery(query);
                if (rs.next()) {
                    vertex = vertResultSetToMap(tableName, rs);
                    break;
                }
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("failed to get vertex by id");
        }

        return vertex; 
    };

    /**
     * create an edge given two vertex IDs with a specific relationship type
     * @param inVertID - in vertex ID
     * @param outVertID - out vertex ID
     * @param relation - relationship type
     */
    @Override
    public void addEdge(String inVertID, String outVertID, String relation) {
        sanityCheck("add edge", inVertID, "inVertID");
        sanityCheck("add edge", outVertID, "outVertID");
        sanityCheck("add edge", relation, "relation");

        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.ADD_EDGE);

        try {
            String outVertTable = getVertexType(outVertID);
            String inVertTable = getVertexType(inVertID);
            preparedStatement.setString(1, relation);
            preparedStatement.setObject(2, outVertID);
            preparedStatement.setObject(3, inVertID);
            preparedStatement.setString(4, outVertTable);
            preparedStatement.setString(5, inVertTable);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to add edge!");
        }  
    };

    private String getVertexType(String vertID) throws SQLException {
        String vertexType = null;
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            PreparedStatement preparedStatement = ps.getPreparedStatement(tableName, API.GET_VERTEX_TYPE_BY_ID);
            preparedStatement.setObject(1, vertID);
            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                vertexType = rs.getString(1);
                break;
            }
        }

        return vertexType;
    }
 
    /**
     * returns list of edge info maps for the outgoing edges of this vertex
     * @param outVertID
     * @return list of edge property maps with matching outVertID
     */
    @Override
    public List<Map<String, Object>> getOutEdges(String outVertID) {
        sanityCheck("get edges", outVertID, "outVertID");

        List<Map<String, Object>> edges = new ArrayList<Map<String, Object>>();
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_OUT_EDGES);
        try {
            preparedStatement.setObject(1, outVertID);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                edges.add(map);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to get out edges!");
        }
        return edges;
    };

    /**
     * returns list of edge info maps for the outgoing edges of this vertex
     * @param outVertID
     * @return list of edge property maps with matching outVertID
     */
    @Override
    public List<Map<String, Object>> getOutEdgesPage(String outVertID, int offset, int limit) {
        sanityCheck("get edges", outVertID, "outVertID");

        List<Map<String, Object>> edges = new ArrayList<Map<String, Object>>();
        String query = String.format("SELECT * FROM Edges WHERE outVertID ='%s' order by timestamp, inVertID desc offset %d limit %d;", outVertID, offset, limit);
        try {
            Connection connection = connectionPool.getConnection();
            Statement poolStatement = connection.createStatement(); 
            ResultSet rs = poolStatement.executeQuery(query);
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                edges.add(map);
            }
            connection.close();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to get out edges!");
        }
        return edges;
    };

    /**
     * returns list of edge info maps for the incoming edges of this vertex
     * @param inVertID
     * @return list of edge property maps with matching inVertID
     */
    @Override
    public List<Map<String, Object>> getInEdges(String inVertID) {
        sanityCheck("get edges", inVertID, "inVertID");

        List<Map<String, Object>> edges = new ArrayList<Map<String, Object>>();
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_IN_EDGES);

        try {
            preparedStatement.setObject(1, inVertID);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                edges.add(map);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to get in edges!");
        }

        return edges;     
    }

    /**
     * returns list of edge info maps for the incoming edges of this vertex
     * @param inVertID
     * @return list of edge property maps with matching inVertID
     */
    @Override
    public List<Map<String, Object>> getInEdgesPage(String inVertID, int offset, int limit) {
        sanityCheck("get edges", inVertID, "inVertID");

        List<Map<String, Object>> edges = new ArrayList<Map<String, Object>>();
        String query = String.format("SELECT * FROM Edges WHERE inVertID ='%s' order by timestamp, outVertID desc offset %d limit %d;", inVertID, offset, limit);
        try {
            Connection connection = connectionPool.getConnection();
            Statement poolStatement = connection.createStatement(); 
            ResultSet rs = poolStatement.executeQuery(query);
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                edges.add(map);
            }
            connection.close();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to get in edges!");
        }

        return edges;     
    }
    
    /**
     * execute query selecting edges based on some constraints
     * @param query - select query with some constraints
     * @return list of maps - list of selected edges
     */
    private List<Map<String, Object>> getEdges(String query) {
        List<Map<String, Object>> edges = new ArrayList<Map<String, Object>>();
        try {
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                edges.add(map);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("failed to select edges with query: " + query);
        }    

        return edges;
    }

    /**
     * Identify the vertices where their relationship type and direction enter the specified vertex
     * @param outVertID - vertex end point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    @Override
    public List<String> getInVertIDsByRelation(String outVertID, String relation) {
        sanityCheck("get vert id", outVertID, "outVertID");
        sanityCheck("get vert id", relation, "relation");

        List<String> vertIDs =  new ArrayList<String>();
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_IN_VERT_IDS_BY_RELATION);
        try {
            preparedStatement.setString(1, relation);
            preparedStatement.setObject(2, outVertID);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                vertIDs.add(rs.getString("vertID"));
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to get in vert ids!");
        }

        return vertIDs;
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
    public List<String> getInVertIDsByRelation(String outVertID, String relation, List<DBConstraint> constraints) {
        sanityCheck("get vert", outVertID, "vertID");
        sanityCheck("get vert", relation, "relation");
        sanityCheck(constraints);

        List<String> vertIDs = new ArrayList<String>();
        String constraintsList = buildConstraintsSubquery(constraints);
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_IN_VERT_IDS_AND_TABLE_BY_RELATION);
        try {
            preparedStatement.setString(1, relation);
            preparedStatement.setObject(2, outVertID);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                String vertID = rs.getString(1);
                String tableName = rs.getString(2);
                String query = buildString("SELECT _id AS vertID FROM ", tableName, " WHERE _id = '", vertID, "' AND ", constraintsList, ";");
                vertIDs.addAll(getVertIDs(query));   
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to get in vert ids by relation and constraints!");
        }

        return vertIDs;
    };

    /**
     * Identify the vertices where their relationship type and direction leave the specified vertex
     * @param v1 - vertex starting point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    @Override
    public List<String> getOutVertIDsByRelation(String inVertID, String relation) {
        sanityCheck("get edge", inVertID, "inVertID");
        sanityCheck("get edge", relation, "relation");

        List<String> vertIDs =  new ArrayList<String>();
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_OUT_VERT_IDS_BY_RELATION);
        try {
            preparedStatement.setString(1, relation);
            preparedStatement.setObject(2, inVertID);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                vertIDs.add(rs.getString("vertID"));
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to get out vert ids!");
        }

        return vertIDs;
    };

    /**
     * Identify the vertices where their relationship type and direction leave the specified vertex
     * and where the found vertices match the following constraints
     * @param vertID - vertex starting point
     * @param relation - the relationship type of the edge
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    @Override
    public List<String> getOutVertIDsByRelation(String inVertID, String relation, List<DBConstraint> constraints) {
        sanityCheck("get vert", inVertID, "vertID");
        sanityCheck("get vert", relation, "relation");
        sanityCheck(constraints);

        List<String> vertIDs = new ArrayList<String>();
        String constraintsList = buildConstraintsSubquery(constraints);
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_OUT_VERT_IDS_AND_TABLE_BY_RELATION);
        try {
            preparedStatement.setString(1, relation);
            preparedStatement.setObject(2, inVertID);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                String vertID = rs.getString(1);
                String tableName = rs.getString(2);
                String query = buildString("SELECT _id AS vertID FROM ", tableName, " WHERE _id = '", vertID, "' AND ", constraintsList, ";");
                vertIDs.addAll(getVertIDs(query));   
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to get out vert ids by relation and constraints!");
        }

        return vertIDs;
    };

    /**
     * Identify all vertices where their relationship type and direction either enter or leave the specified vertex
     * @param vertID - vertex starting or ending point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    @Override
    public List<String> getVertIDsByRelation(String vertID, String relation) {
        sanityCheck("get vert id", vertID, "vertID");
        sanityCheck("get vert id", relation, "relation");

        List<String> vertIDs =  new ArrayList<String>();
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_VERT_IDS_BY_RELATION);
        try {
            preparedStatement.setString(1, relation);
            preparedStatement.setObject(2, vertID);
            preparedStatement.setString(3, relation);
            preparedStatement.setObject(4, vertID);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                vertIDs.add(rs.getString("vertID"));
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to get vert ids!");
        }

        return vertIDs;
    };

    /**
     * Identify all vertices where their relationship type and direction either enter or leave the specified vertex
     * and where the found vertices match the following constraints
     * @param vertID - vertex starting or ending point
     * @param relation - the relationship type of the edge
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    @Override
    public List<String> getVertIDsByRelation(String vertID, String relation, List<DBConstraint> constraints) {
        sanityCheck("get vert", vertID, "vertID");
        sanityCheck("get vert", relation, "relation");
        sanityCheck(constraints);

        List<String> vertIDs = getVertIDsByRelation(vertID, relation);
        String idList = buildIDListSubquery(vertIDs);
        String constraintsList = buildConstraintsSubquery(constraints);
        
        vertIDs.clear();
        List<String> columnList = getConstraintProperties(constraints);
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            if (containsAllColumns(tableName, columnList)) {
                String query = buildString("SELECT _id AS vertID FROM ", tableName, " WHERE _id IN(", idList, ") AND ", constraintsList, ";");
                vertIDs.addAll(getVertIDs(query));
            }
        }


        return vertIDs;
    };
     
    /** 
     * Perform a query/search of the DB using the following constraints on the request
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    @Override
    public List<String> getVertIDsByConstraints(List<DBConstraint> constraints) {
        sanityCheck(constraints);

        List<String> vertIDs = new ArrayList<String>();
        //TODO: check if constraints contain vertexType to avoid searching all tables
        String constraintsList = buildConstraintsSubquery(constraints);
        List<String> columnList = getConstraintProperties(constraints);
        if (columnList.contains("vertexType")) {
            for (int i = 0; i < constraints.size(); i++) {
                DBConstraint constraint = constraints.get(i);
                String key = constraint.getProp();
                if (key.equals("vertexType")) {
                    String tableName = constraint.getVal().toString().replaceAll("'", "");
                    String query = buildString("SELECT _id AS vertID FROM ", tableName, " WHERE ", constraintsList, ";");
                    vertIDs.addAll(getVertIDs(query));
                    break;
                }
            }
        } else {
            for (Object table : vertTables.keySet()) {
                String tableName = table.toString();
                if (containsAllColumns(tableName, columnList)) {
                    String query = buildString("SELECT _id AS vertID FROM ", tableName, " WHERE ", constraintsList, ";");
                    vertIDs.addAll(getVertIDs(query));
                }
            }
        }
        
        return vertIDs;
    };

    /** 
     * Perform a query/search of the DB using the following constraints on the request
     * @param constraints - list of constraint objects
     * @param offset - offset from start
     * @param limit - number of records
     * @return list of vertex IDs
     */
    public List<String> getVertIDsByConstraints(List<DBConstraint> constraints, int offset, int limit) {
        sanityCheck(constraints); 

        List<String> vertIDs = new ArrayList<String>();
        //TODO: check if constraints contain vertexType to avoid searching all tables
        String constraintsList = buildConstraintsSubquery(constraints);
        List<String> columnList = getConstraintProperties(constraints);
        if (columnList.contains("vertexType")) {
            for (int i = 0; i < constraints.size(); i++) {
                DBConstraint constraint = constraints.get(i);
                String key = constraint.getProp();
                if (key.equals("vertexType")) {
                    String tableName = constraint.getVal().toString().replaceAll("'", "");
                    if (columnList.contains("name")) {
                        JSONObject tableColumns = vertTables.getJSONObject(tableName).getJSONObject("columns");
                        if (tableColumns.has("alias")) {
                            Object name = getNameConstraint(constraints);
                            constraintsList = buildString(constraintsList, " OR ", buildConstraintsSubquery(getConstraint("alias", Condition.contains, name)));
                        }
                    }
                    String query = buildString("SELECT _id as vertID FROM ", tableName, " WHERE ", constraintsList, " order by date(timestamp), name desc offset ", offset, " LIMIT ", limit);
                    vertIDs.addAll(getVertIDs(query));
                    break;
                }
            }
        } else {
            Object name = null;
            if (columnList.contains("name")) {
                name = getNameConstraint(constraints);
            }
            for (Object table : vertTables.keySet()) {
                String tableName = table.toString();
                if (containsAllColumns(tableName, columnList)) {
                    if (name != null) {
                        JSONObject tableColumns = vertTables.getJSONObject(tableName).getJSONObject("columns");
                        if (tableColumns.has("alias")) {
                            String query = buildString("SELECT _id as vertID FROM ", tableName, " WHERE ", constraintsList, " OR ", buildConstraintsSubquery(getConstraint("alias", Condition.contains, name)), " order by timestamp, name desc offset ", offset, " LIMIT ", limit);
                            vertIDs.addAll(getVertIDs(query));
                        } else {
                            String query = buildString("SELECT _id as vertID FROM ", tableName, " WHERE ", constraintsList, " order by date(timestamp), name desc offset ", offset, " LIMIT ", limit);
                            vertIDs.addAll(getVertIDs(query));
                        }
                    } else {
                        String query = buildString("SELECT _id as vertID FROM ", tableName, " WHERE ", constraintsList, " order by date(timestamp), name desc offset ", offset, " LIMIT ", limit);
                        vertIDs.addAll(getVertIDs(query));
                    }
                }
            }
        }
        
        return vertIDs;
    };

    private Object getNameConstraint(List<DBConstraint> constraints) {
        Object name = null;
        for (int i = 0; i < constraints.size(); i++) {
            DBConstraint constraint = constraints.get(i);
            String key = constraint.getProp();
            if (key.equals("name")) {
                name = constraint.getVal();
                break;
            }
        }

        return name;
    }

    private Map<String, Object> getConstraintPropertiesMap(List<DBConstraint> constraints) {
        Map<String, Object> map = new HashMap<String, Object>();
        for (DBConstraint constraint : constraints) {
            map.put(constraint.getProp(), constraint.getVal());
        }

        return map;
    }

    private List<String> getConstraintProperties(List<DBConstraint> constraints) {
        List<String> list = new ArrayList<String>();
        for (DBConstraint constraint : constraints) {
            list.add(constraint.getProp());
        }

        return list;
    }

    /**
     * build portion of query like: " _id IN ('value1', 'value2', 'value3') "
     * used for queries where id is part of a constraints
     */
    private String buildIDListSubquery(List<String> list) {
        StringBuilder idList = new StringBuilder();
        String delimiter = "";
        for (String id : list) {
            idList
                .append(buildString(delimiter, "'", id, "'"));
            delimiter = ", ";
        }

        return idList.toString();
    }

    /**
     * build constrains posrtion (substring) of querty like: " WHERE property = 'value' AND _id != 'some-id-value' "
     */
    private String buildConstraintsSubquery(List<DBConstraint> constraints) {
        StringBuilder constraintsList = new StringBuilder();
        String delimiter = "";
        for (int i = 0; i < constraints.size(); i++) {
            DBConstraint constraint = constraints.get(i);
            String cond = constraint.condString(constraint.getCond());
            String key = constraint.getProp();
            Object value = constraint.getVal();
            constraintsList.append(buildString(delimiter, key, " ", cond, " ", value));
            delimiter = " AND ";
        }

        return constraintsList.toString();
    }

    private String buildConstraintsSubquery(DBConstraint constraint) {
        String cond = constraint.condString(constraint.getCond());
        String key = constraint.getProp();
        Object value = constraint.getVal();

        return buildString(key, " ", cond, " ", value);
    }

    private boolean containsAllColumns(String tableName, List<String> columnNames) {
        Set tableColumns = vertTables.getJSONObject(tableName).getJSONObject("columns").keySet();
        boolean contains = tableColumns.containsAll(columnNames); 

        return contains;
    } 
    
    /**
     * Given two vertices and a relation, remove the edge
     * @param inVertID - in vertex ID
     * @param outVertID - out vertex ID
     * @param relation - relationship type
     */
    @Override
    public void removeEdgeByRelation(String inVertID, String outVertID, String relation) {
        sanityCheck("remove edge", inVertID, "inVertID");
        sanityCheck("remove edge", outVertID, "outVertID");
        sanityCheck("remove edge", relation, "relation");

        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.REMOVE_EDGE_BY_RELATION);
        try {
            preparedStatement.setString(1, relation);
            preparedStatement.setObject(2, outVertID);
            preparedStatement.setObject(3, inVertID);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to remove edge!");
        }
    };

    /**
     * Given a specific vertex remove it from the DB
     * (Note any edges connected to it will also be removed)
     * @param vertID - vertex ID
     */
    @Override
    public void removeVertByID(String vertID) {
        sanityCheck("remove vertex", vertID, "vertID");

        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            PreparedStatement preparedStatement = ps.getPreparedStatement(tableName, API.REMOVE_VERT_BY_ID);
            try {
                preparedStatement.setObject(1, vertID);
                int count = preparedStatement.executeUpdate();
                if (count != 0) {
                    removeEdgeByVertID(vertID);
                    break;
                }
            } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
                //throw new StuccoDBException("Failed to remove vert by id!");
            }
        }

    };

    /**
     * remove edge if it contains outVertID or inVertID matching vertID
     */
    private void removeEdgeByVertID(String vertID) {
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.REMOVE_EDGE_BY_VERT_ID);
        try {
            preparedStatement.setObject(1, vertID);
            preparedStatement.setObject(2, vertID);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to remove edge by vert ID!");
        }
    }
    
    /**
     * Replaces current vertex's property map with a different one
     * @param vertID - vertex that will be changed
     * @param properties - property map with different contents, complete replacement of current content
     */
    @Override
    public void updateVertex(String vertID, Map<String, Object> properties) {
        sanityCheck("update vertex", properties, "name", "vertexType");
        
        String delimiter = "";
        StringBuilder updates = new StringBuilder();
        for (String property : properties.keySet()) {
            String value = normalizeValue(properties.get(property));
            updates.append(buildString(delimiter, property, " = ", value));
            delimiter = ", ";
        }
        String tableName = properties.get("vertexType").toString();
        String query = buildString("UPDATE ", tableName, " SET ", updates, " WHERE _id = '", vertID, "';");

        executeSQLUpdate(query);
    };

     /**
     * inserts this properties new value into the specified vertex ID and key
     * @param vertID
     * @param key
     * @param newValue - All multi-value types will be Expected to be of type SET or SINGLE
     */
    @Override
    protected void setPropertyInDB(String vertID, String key, Object newValue) {
        String value = normalizeValue(newValue);
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            String query = buildString("UPDATE ", tableName, " SET ", key, " = ", value, ";");
            boolean success = executeSQLUpdate(query);
            if (success) {
                break;
            }
        }
    };

    /**
     * convert value to string, format acceptable by SQL query according to db constraints
     */
    private static String normalizeValue(Object value) {
        String returnValue = null;
        if (value instanceof Collection) {
            returnValue = buildArrayString((Collection)value);
        } else {
            returnValue = buildString("'", value, "'");
        }

        return returnValue;
    }


    /**
     * turn list into string of following form (including double quotes): "{'value1', 'value2'}";
     * NOTE: function treats all list values as strings for now
     * @param list - list of values to be inserted into table
     * @return string representation of list values acceptable by sql
     */
    private static String buildArrayString(Collection list) {
        String delimiter = "";
        StringBuilder sb = new StringBuilder("'{");
        for (Object value : list) {
            sb.append(buildString(delimiter, "\"", value, "\""));
            delimiter = ", ";
        }
        sb.append("}'");

        return sb.toString();
    }

    /**
     * remove all vertices in the databases
     */
    @Override
    public void removeAllVertices() {
        try {
            for (Object table : vertTables.keySet()) {
                String tableName = table.toString();
                PreparedStatement preparedStatement = ps.getPreparedStatement(tableName, API.REMOVE_ALL_VERTICES);
                preparedStatement.executeUpdate();
            }
            PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.REMOVE_ALL_EDGES);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("Failed to remove all vertices!");            
        }
        
    };

    /**
     * load db state from the specified file
     */
    @Override
    public void loadState(String filePath) {
        try {
            InputStream is = new FileInputStream(filePath);
            String textContents = IOUtils.toString(is);
            is.close();

            JSONObject contents = new JSONObject(textContents);
            JSONObject vertsJSON = contents.getJSONObject("vertices");
            JSONArray edgesJSON = contents.getJSONArray("edges");
            //add vertices
            for(Object id : vertsJSON.keySet()) {
                JSONObject jsonVert = vertsJSON.getJSONObject(id.toString());
                String description = jsonVert.optString("description");
                if (description != null && !description.isEmpty()) {
                    //This is kind of an odd workaround, to prevent ui from treating, eg, "URI: www.blah.com | Type: URL |" as a URL instead of a string.
                    //TODO: this is really a problem in the UI, as far as we care it's still just a string either way.
                    jsonVert.put("description", " " + description);
                } else {
                    //ui assumes everything has a description, this is a workaround to avoid having empty text in various places.
                    jsonVert.put("description", jsonVert.optString("name"));
                }
                Map<String, Object> vert = jsonVertToMap(jsonVert);
                addVertex(vert);
            }
            //add edges.
            for (int i = 0; i < edgesJSON.length(); i++) {
                JSONObject edge = edgesJSON.getJSONObject(i);
                try {
                    String inVertID = edge.getString("inVertID");
                    String outVertID = edge.getString("outVertID");
                    String relation = edge.getString("relation");
                    int matchingEdgeCount = getEdgeCountByRelation(inVertID, outVertID, relation);
                    if (matchingEdgeCount == 0) {
                        addEdge(inVertID, outVertID, relation); 
                    }
                } catch (JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IllegalArgumentException e) {
                    // TODO Auto-generated catch block
                    System.err.println("error when loading edge: " + edge);
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    /**
     * save db state to the specified file
     */
    @Override
    public void saveState(String filePath) {
        try {
            OutputStream os = new FileOutputStream(filePath);
            PrintStream printStream = new PrintStream(os);

            JSONObject vertsJSON = getAllVertices();
            JSONArray edgesJSON = getAllEdges();

            JSONObject contents = new JSONObject();
            contents.put("vertices", vertsJSON);
            contents.put("edges", edgesJSON);

            printStream.print(contents.toString(2));
            printStream.close();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    /**
     * return all vertices, that are currently in DB
     */
    private JSONObject getAllVertices() {
        JSONObject vertices = new JSONObject();
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            PreparedStatement preparedStatement = ps.getPreparedStatement(tableName, API.GET_ALL_VERTICES);
            try {
                ResultSet rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    Map<String, Object> vert = vertResultSetToMap(tableName, rs);
                    JSONObject jsonVert = new JSONObject();
                    for (String key : vert.keySet()) {
                        jsonVert.put(key, vert.get(key));
                    }
                    vertices.put(rs.getString("_id"), jsonVert);
                }
            } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
                //throw new StuccoDBException("failed to select all vertices from DB");
            }
        }

        return vertices;
    }

    /**
     * return all edges, that are currently in DB
     */
    private JSONArray getAllEdges() {
        JSONArray edgesJson = new JSONArray();
        try {
            PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_ALL_EDGES);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                Map<String, Object> edge = edgesResultSetToMap(rs);
                edgesJson.put(new JSONObject(edge));
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("failed to get all edges from DB!");
        }
       
       return edgesJson;
    }

    /**
     * gets the number of vertices in the graph; aka number of rows in all vertex tables in the database
     */
    @Override
    public long getVertCount() {
        long count = 0L; 
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            PreparedStatement preparedStatement = ps.getPreparedStatement(tableName, API.GET_VERT_COUNT);
            try {
                ResultSet rs = preparedStatement.executeQuery();
                if (rs.next()) {
                    count += rs.getLong(1);
                }
            } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
                //throw new StuccoDBException("failed to count rows in table: " + tableName);
            }
        }
        
        return count;
    };
 
    @Override
    public long getVertCountByConstraints(List<DBConstraint> constraints) {
        sanityCheck(constraints);
        long count = 0L;

        for (int i = 0; i < constraints.size(); i++) {
            DBConstraint c = constraints.get(i);
            String val = (String) c.getVal();
            if (val.startsWith("'(current")) {
                val = val.substring(1, val.length() - 1);
                constraints.set(i, new PostgresqlDBConstraint(c.getProp(), c.getCond(), val));
            }
        }

        String constraintsString = buildConstraintsSubquery(constraints);
        List<String> constraintsList = getConstraintProperties(constraints);

        //quering only once, since constraints contain vertexType aka. table name
        if (constraintsList.contains("vertexType")) {
            for (int i = 0; i < constraints.size(); i++) {
                DBConstraint constraint = constraints.get(i);
                String key = constraint.getProp();
                if (key.equals("vertexType")) { 
                    String tableName = constraint.getVal().toString().replaceAll("'", "");
                    String query = buildString("SELECT count(*) as count FROM ", tableName, " WHERE ", constraintsString);
                    try {
                        count = executeCountQuery(query);
                    } catch (StuccoDBException e) {

                    }
                    break;
                }
            }
        } else {
        //quering all tables
            for (Object table : vertTables.keySet()) {
                String tableName = table.toString();
                if (containsAllColumns(tableName, constraintsList) || (constraintsList.size() == 1 && constraintsList.contains("date(timestamp)"))) {
                    String query = buildString("SELECT count(*) as count FROM ", tableName, " WHERE ", constraintsString);
                    count = count + executeCountQuery(query);
                }
            }
        }

        return count;
    };

    private long executeCountQuery(String query) {
        long count = 0L;

        try {
            Connection connection = connectionPool.getConnection();
            Statement statement = connection.createStatement(); 
            ResultSet rs = statement.executeQuery(query);
            if (rs.next()) {
                count = rs.getLong("count");
            }
            connection.close();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("failed to execute query: " + query);
        }

        return count;
    }
    
    /**
     * gets the number of edges in the graph; aka number of rows in "Edges" table in the database
     */
    @Override
    public long getEdgeCount() {
        long count = 0L;
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_EDGE_COUNT);
        try {
            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("failed to count rows in Edge table");
        }

        return count;
    };

    /**
     * gets the number of edges in the graph; aka number of rows in "Edges" table in the database
     */
    @Override
    public long getInEdgeCount(String inVertID) {
        String query = buildString("SELECT COUNT(*) FROM Edges WHERE inVertID = '" + inVertID + "';");
        long count = executeCountQuery(query);

        return count;
    };

    /**
     * gets the number of edges in the graph; aka number of rows in "Edges" table in the database
     */
    @Override
    public long getOutEdgeCount(String outVertID) {
        String query = buildString("SELECT COUNT(*) FROM Edges WHERE outVertID = '" + outVertID + "';");
        long count = executeCountQuery(query);

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
        sanityCheck("get edge count", relation, "relation");
        sanityCheck("get edge count", inVertID, "inVertID");
        sanityCheck("get edge count", outVertID, "outVertID");

        int count = 0;
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_EDGE_COUNT_BY_RELATION);
        try {
            preparedStatement.setString(1, relation);
            preparedStatement.setObject(2, outVertID);
            preparedStatement.setObject(3, inVertID);
            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("failed to count rows in Edge table");
        }

        return count;
    };
    

    /**
     * helper funciton to collect list of ids selected by query
     * @param query - select Edge table query with some constraints
     * @return list of vert ids selected by query
     */
    private List<String> getVertIDs(String query) {
        List<String> vertIDs = new ArrayList<String>();
        try {
            Connection connection = connectionPool.getConnection();
            Statement statement = connection.createStatement(); 
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                vertIDs.add(rs.getString("vertID"));
            }
            connection.close();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("failed to select vertIDs with query: " + query);
        }

        return vertIDs;
    }

    /**
     *  helper function to execute sql queries when return is not reqired
     */
    private boolean executeSQLQuery(String query) {
        boolean success = false;
        try {
            ResultSet rs = statement.executeQuery(query);
            success = rs.next();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("failed to execute query: " + query);
        }

        return success;
    }

    /**
     *  helper function to execute sql queries when return is not reqired
     */
    private boolean executeSQLUpdate(String query) {
        boolean success = false;
        try {
            int count = statement.executeUpdate(query);
            success = (count != 0);
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            //throw new StuccoDBException("failed to execute query: " + query);
        }

        return success;
    }

    /**
     * concatenates multiple substrings 
     */
    private static String buildString(Object... substrings) {
        StringBuilder str = new StringBuilder();
        for (Object substring : substrings) {
            str.append(substring);
        }

        return str.toString();
    }

    /**
     * convert constraint value to query accepted format
     * @param property name
     * @param condition 
     * @param value
     */
    @Override
    public DBConstraint getConstraint(String property, Condition condition, Object value) {
        System.out.println("in get constraintsList with value: " + value);
        if (condition == Condition.substring) {
            value = buildString("'%", value, "%'");
        } else if (condition == Condition.contains) {
            if (value instanceof String) {
                String str = value.toString();
                if (!str.startsWith("\'")) {
                    value = buildString("'", value, "'");
                }
            }
            value = buildString("ARRAY[", value, "]");
        } else {
            value = buildString("'", value, "'");
        }

        return new PostgresqlDBConstraint(property, condition, value);
    };
 
    /**
     * take ResultSet from sql query and converts it into HashMap<String, Object>
     * @param rs - ResultSet containing all sql selected values
     * @param tableName - name of a table that was quered, required to check if value is an array
     * @return map - map with key = column name and value = column value
     */
    private Map<String, Object> vertResultSetToMap(String tableName, ResultSet rs) throws SQLException {
        Map<String, Object> map = new HashMap<String, Object>();
        JSONArray order = vertTables.getJSONObject(tableName).getJSONArray("order");
        for (int i = 3; i < order.length(); i++) {
        // for (Object column : table.keySet()) {
            String columnName = order.getString(i);
            if (Columns.valueOf(columnName).type == TYPE.ARRAY) {
                Array array = rs.getArray(columnName);
                if (array != null) {
                    Set value = new HashSet<Object>(Arrays.asList((Object[]) array.getArray()));
                    if (!value.isEmpty()) {
                        map.put(columnName, value);
                    }
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
     * convert ResultSet from quering Edges table to Map<String, Object>
     */
    private static Map<String, Object> edgesResultSetToMap(ResultSet rs) throws SQLException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("outVertID", rs.getString("outVertID"));
        map.put("inVertID", rs.getString("inVertID"));
        map.put("relation", rs.getString("relation"));
        map.put("outVertTable", rs.getString("outVertTable"));
        map.put("inVertTable", rs.getString("inVertTable"));

        return map;
    }

    /**
     * argument sanity check for null or empty string
     * @param action - goal of function, that received this argument 
     * @param argValue - argument value
     * @param argName - argument name for descriptive exception message
     */
    private static void sanityCheck(String action, String argValue, String argName) throws IllegalArgumentException {
        if (argValue == null || argValue.isEmpty()) {
            throw new IllegalArgumentException("cannot " + action + " with missing or invalid " + argName);
        }
    }

    /**
     * argument sanity check for null or empty string
     * @param action - goal of function, that received this argument 
     * @param properties - map of properties and property values
     * @param propertyNames - property names to perform sanity check on
     */
    private static void sanityCheck(String action, Map<String, Object> properties, String... propertyNames) {
        for (String name : propertyNames) {
            if (!properties.containsKey(name)) {
                throw new IllegalArgumentException("cannot " + action + " with missing or invalid " + name);
            }
        }
    }

    /**
     * sanity check for passed non instantiated list
     */
    private static void sanityCheck(List list) {
        if (list == null) {
            throw new NullPointerException();
        }
    }

    private static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);

        return sw.toString();
    }

    @Override
    public Map<String, Object> jsonVertToMap(JSONObject v) {
        Map<String, Object> vert = new HashMap<String, Object>();
        for(Object k : v.keySet()) {
            String column = k.toString();
            Object value = v.get(column);
            switch (Columns.valueOf(column).type) {
                case ARRAY:
                    value = jsonArrayToSet((JSONArray)value);
                    break;
            //    case LONG:
            //        value = ((Number)value).longValue(); 
            //        break;
            }
            vert.put(column, (Object) value);
        }

        return vert;
    }

    /**
     * converts a JSONArray to a Set
     * @param a
     * @return
     */
    static protected Set<Object> jsonArrayToSet(JSONArray a){
        Set<Object> s = new HashSet<Object>();
        for(int i=0; i<a.length(); i++){
            s.add(a.get(i));
        }
        return s;
    }

    /**
     * bulk loading of graph (vertices and edges):
     * 1. create table 'temp'
     * 2. use 'copy' to copy graph into 'temp' table
     * 3. calling 'merge_graph()' funciton that will perform alignment of new vertices and edges with existing
     * 4. deleting 'temp' table
     * 
     * @param graph - json object of vertices and edges
     */
    @Override
    public void bulkLoadGraph(JSONObject graph) {     
        String generatedTableName = RandomStringUtils.randomAlphabetic(10);

        CopyManager copyManager = null;
        CopyIn copyIn = null;
        try {
            copyManager = new CopyManager((BaseConnection)connection);
            statement.executeUpdate(buildString("CREATE TABLE IF NOT EXISTS ", generatedTableName, " (graph json);"));
            copyIn = copyManager.copyIn(buildString("COPY ", generatedTableName, " FROM STDIN WITH csv quote e'\\x01' delimiter e'\\x02';"));
            byte[] bytes = graph.toString().getBytes("UTF-8");
            copyIn.writeToCopy(bytes, 0, bytes.length);
            copyIn.endCopy();
            statement.execute(buildString("SELECT merge_graph('", generatedTableName, "');"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } 
        finally {
            try {
                if (copyIn.isActive()) {
                    copyIn.cancelCopy();
                }
                statement.execute(buildString("DROP TABLE IF EXISTS ", generatedTableName, ";"));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } 
    }
}
