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
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.NumberUtils;

import org.json.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresqlDBConnection extends DBConnectionBase {
    
    private static final String POSTGRESQL_TABLES = "postgresql/tables.json";
    private static final String[] columnOrder = {"vertexType", "observableType", "name", "alias", "ipInt", "startIP", 
        "startIPInt", "endIP", "endIPInt", "description", "shortDescription", "details", "source", "sourceDocument", "location", "publishedDate"};
    private static final JSONObject vertTables;
    private static Logger logger;
    private Map<String, Object> configuration;
    private Connection connection;
    private Statement statement;

    static {
        try {
            JSONObject tables = new JSONObject(IOUtils.toString(PostgresqlDBConnection.class.getClassLoader().getResourceAsStream(POSTGRESQL_TABLES), "UTF-8"));
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
        String query = ("CREATE TABLE IF NOT EXISTS Edges (relation text NOT NULL, outVertID uuid NOT NULL, inVertID uuid NOT NULL);");
        statement.executeUpdate(query);
    }

    private String buildCreateTableSQL(String tableName, JSONObject table) {
        String delimiter = "";
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sql.append(tableName);
        sql.append(" (_id uuid PRIMARY KEY DEFAULT uuid_generate_v4() NOT NULL UNIQUE, ");
        for (String columnName : columnOrder) {
            if (table.has(columnName)) {
                sql.append(delimiter);
                String constraint = table.getJSONObject(columnName).getString("constraint");
                sql.append(columnName);
                sql.append(" ");
                sql.append(constraint);
                delimiter = ", ";
            }
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
        sanityCheck("add vertex", properties, "name", "vertexType");

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
        String delimiter = "";
        JSONObject table = vertTables.getJSONObject(tableName);
        StringBuilder propertiesSQL = new StringBuilder();
        StringBuilder valuesSQL = new StringBuilder();
        for (String propertyName : properties.keySet()) {
            propertiesSQL.append(delimiter);
            valuesSQL.append(delimiter);
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
            delimiter = ", ";
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
        String delimiter = "";
        StringBuilder sb = new StringBuilder("'{");
        for (String value : list) {
            sb.append(delimiter);
            sb.append("\"");
            sb.append(value);
            sb.append("\"");
            delimiter = ", ";
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
        sanityCheck("get vert", id, "id");

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
        sanityCheck("add edge", inVertID, "inVertID");
        sanityCheck("add edge", outVertID, "outVertID");
        sanityCheck("add edge", relation, "relation");
       
        String query = new StringBuilder()
            .append("INSERT INTO Edges (relation, outVertID, inVertID) VALUES ('")
            .append(relation)
            .append("', '")
            .append(outVertID)
            .append("', '")
            .append(inVertID)
            .append("');")
            .toString();
        executeSQLQuery(query);   
    };

    /**
     * returns list of edge info maps for the outgoing edges of this vertex
     * @param outVertID
     * @return list of edge property maps with matching outVertID
     */
    @Override
    public List<Map<String, Object>> getOutEdges(String outVertID) {
        sanityCheck("get edges", outVertID, "outVertID");
        
        StringBuilder query = new StringBuilder()
            .append("SELECT * FROM Edges WHERE outVertID = '")
            .append(outVertID)
            .append("';");

        List<Map<String, Object>> outEdges = getEdges(query.toString());

        return outEdges;
    };

    /**
     * returns list of edge info maps for the incoming edges of this vertex
     * @param inVertID
     * @return list of edge property maps with matching inVertID
     */
    @Override
    public List<Map<String, Object>> getInEdges(String inVertID) {
        sanityCheck("get edges", inVertID, "inVertID");

        StringBuilder query = new StringBuilder()
            .append("SELECT * FROM Edges WHERE inVertID = '")
            .append(inVertID)
            .append("';");

        List<Map<String, Object>> inEdges = getEdges(query.toString());

        return inEdges;
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
            throw new StuccoDBException("failed to select edges with query: " + query);
        }    

        return edges;
    }

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
     * @param outVertID - vertex end point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    @Override
    public List<String> getInVertIDsByRelation(String outVertID, String relation) {
        sanityCheck("get vert id", outVertID, "outVertID");
        sanityCheck("get vert id", relation, "relation");
        
        String query = new StringBuilder()
            .append("SELECT inVertID FROM Edges WHERE relation = '")
            .append(relation)
            .append("' AND outVertID = '")
            .append(outVertID)
            .append("';")
            .toString();
        List<String> inVertIDsList = getVertIDs(query);

        return inVertIDsList;
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

        String query = new StringBuilder()
            .append("SELECT outVertID FROM Edges WHERE relation = '")
            .append(relation)
            .append("' AND inVertID = '")
            .append(inVertID)
            .append("';")
            .toString();
        List<String> outVertIDsList = getVertIDs(query);

        return outVertIDsList;
    };

    /**
     * helper funciton to collect list of edge ids selected by query
     * @param query - select Edge table query with some constraints
     * @return list of vert ids selected by query
     */
    private List<String> getVertIDs(String query) {
        List<String> vertIDs = new ArrayList<String>();
        try {
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                vertIDs.add(rs.getString(1));
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to select vertIDs with query: " + query);
        }

        return vertIDs;
    }

    /**
     * Identify all vertices where their relationship type and direction either enter or leave the specified vertex
     * @param id - vertex starting or ending point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    @Override
    public List<String> getVertIDsByRelation(String id, String relation) {
        sanityCheck("get vert id", id, "id");
        sanityCheck("get vert id", relation, "relation");

        String query = new StringBuilder()
            .append("SELECT outVertID as vertID FROM Edges WHERE relation = '")
            .append(relation)
            .append("' AND inVertID = '")
            .append(id)
            .append("' UNION ")
            .append("SELECT inVertID as vertID FROM Edges WHERE relation = '")
            .append(relation)
            .append("' and outVertID = '")
            .append(id)
            .append("';")
            .toString();

        List<String> vertIDs = getVertIDs(query);

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
        List<String> inVertIDs = getInVertIDsByRelation(outVertID, relation);
        if (!inVertIDs.isEmpty()) {
            String idIN = buildIDInQuery(inVertIDs);
            String constraintsQuery = buildConstraintQuery(constraints);
            List<String> columnList = getConstraintProperties(constraints);
            for (Object table : vertTables.keySet()) {
                String tableName = table.toString();
                if (containsAllColumns(tableName, columnList)) {
                    String query = new StringBuilder()
                        .append("SELECT _id FROM ")
                        .append(tableName)
                        .append(constraintsQuery)
                        .append(" AND ")
                        .append(idIN)
                        .append(";")
                        .toString();   

                    vertIDs.addAll(getVertIDs(query));
                }
            }
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
        List<String> outVertIDs = getOutVertIDsByRelation(inVertID, relation);
        if (!outVertIDs.isEmpty()) {
            String idIN = buildIDInQuery(outVertIDs);
            String constraintsQuery = buildConstraintQuery(constraints);
            List<String> columnList = getConstraintProperties(constraints);
            for (Object table : vertTables.keySet()) {
                String tableName = table.toString();
                if (containsAllColumns(tableName, columnList)) {
                    String query = new StringBuilder()
                        .append("SELECT _id FROM ")
                        .append(tableName)
                        .append(constraintsQuery)
                        .append(" AND ")
                        .append(idIN)
                        .append(";")
                        .toString();   

                    vertIDs.addAll(getVertIDs(query));
                }
            }
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

        StringBuilder constraintsQuery = new StringBuilder()
            .append(buildConstraintQuery(constraints))
            .append(" AND (");
        String delimiter = "";
        List<String> vertIDs = getVertIDsByRelation(vertID, relation);
        for (String _id : vertIDs) {
            constraintsQuery
                .append(delimiter)
                .append("_id = '")
                .append(_id)
                .append("'");

            delimiter = " OR ";
        }
        constraintsQuery.append(")");

        vertIDs.clear();
        List<String> columnList = getConstraintProperties(constraints);
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            if (containsAllColumns(tableName, columnList)) {
                String query = new StringBuilder()
                    .append("SELECT _id FROM ")
                    .append(tableName)
                    .append(constraintsQuery)
                    .append(";")
                    .toString();   

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

        //TODO: check if constraints contain vertexType to avoid searching all tables
        String constraintsString = buildConstraintQuery(constraints);
        List<String> columnList = getConstraintProperties(constraints);
        List<String> vertIDs = new ArrayList<String>();
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            if (containsAllColumns(tableName, columnList)) {
                String query = new StringBuilder()
                    .append("SELECT _id FROM ")
                    .append(tableName)
                    .append(constraintsString)
                    .append(";")
                    .toString();   

                vertIDs.addAll(getVertIDs(query));
            }
        }
        
        return vertIDs;
    };

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
    private String buildIDInQuery(List<String> idList) {
        StringBuilder in = new StringBuilder(" _id IN ('");
        String delimiter = "";
        for (String id : idList) {
            in
                .append(id)
                .append("'")
                .append(delimiter);
        }
        in.append(")");

        return in.toString();
    }

    /**
     * build constrains posrtion (substring) of querty like: " WHERE property = 'value' AND _id != 'some-id-value' "
     */
    private String buildConstraintQuery(List<DBConstraint> constraints) {
        StringBuilder constrQuery = new StringBuilder(" WHERE ");
        String delimiter = "";
        for (int i = 0; i < constraints.size(); i++) {
            DBConstraint constraint = constraints.get(i);
            String cond = constraint.condString(constraint.getCond());
            String key = constraint.getProp();
            Object value = constraint.getVal();

            constrQuery
                .append(delimiter)
                .append(key)
                .append(" ")
                .append(cond)
                .append(" ")
                .append(value);
            delimiter = " AND ";
        }

        return constrQuery.toString();
    }

    private boolean containsAllColumns(String tableName, List<String> columnNames) {
        boolean contains = true;
        JSONObject table = vertTables.getJSONObject(tableName);
        for (String columnName : columnNames) {
            if (!table.has(columnName)) {
                contains = false;
                break;
            }
        }

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
        
        String query = new StringBuilder()
            .append("DELETE FROM Edges WHERE relation = '")
            .append(relation)
            .append("' AND outVertID = '")
            .append(outVertID)
            .append("' AND inVertID = '")
            .append(inVertID)
            .append("';")
            .toString();
        executeSQLQuery(query);
    };

    /**
     * Given a specific vertex remove it from the DB
     * (Note any edges connected to it will also be removed)
     * @param id - vertex ID
     */
    @Override
    public void removeVertByID(String id) {
        sanityCheck("remove vertex", id, "id");
       
        StringBuilder querySuffix = new StringBuilder()
            .append(" WHERE _id = '")
            .append(id)
            .append("';");
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            StringBuilder query = new StringBuilder("DELETE FROM ")
                .append(tableName)
                .append(querySuffix);
            boolean success = executeSQLQuery(query.toString());
            if (success) {
                removeEdgeByVertID(id);
                break;
            }
        }
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
        sanityCheck("update vertex", properties, "name", "vertexType");
        
        String tableName = properties.get("vertexType").toString();
        String delimiter = "";
        StringBuilder query = new StringBuilder()
            .append("UPDATE ")
            .append(tableName)
            .append(" SET ");
        for (String property : properties.keySet()) {
            Object value = properties.get(property);

            query
                .append(delimiter)
                .append(property)
                .append(" = ")
                .append(convertValueToString(value));
            delimiter = ", ";
        }
        query
            .append(" WHERE _id = '")
            .append(id)
            .append("';");

        executeSQLQuery(query.toString());
    };

     /**
     * inserts this properties new value into the specified vertex ID and key
     * @param id
     * @param key
     * @param newValue - All multi-value types will be Expected to be of type SET or SINGLE
     */
    @Override
    protected void setPropertyInDB(String id, String key, Object newValue) {
        StringBuilder querySuffix = new StringBuilder()
            .append(" SET ")
            .append(key)
            .append(" = ")
            .append(convertValueToString(newValue))
            .append(";");
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            StringBuilder query = new StringBuilder()
                .append("UPDATE ")
                .append(tableName)
                .append(querySuffix);
            boolean success = executeSQLQuery(query.toString());
            if (success) {
                break;
            }
        }
    };

    /**
     * convert value to string, format acceptable by SQL query according to db constraints
     */
    private static String convertValueToString(Object value) {
        String returnValue = null;
        if (value instanceof Collection) {
            returnValue = buildArrayString((List)value);
        } else {
            returnValue = new StringBuilder()
                .append("'")
                .append(value.toString())
                .append("'")
                .toString();
        }

        return returnValue;
    }

    /**
     * remove all vertices in the databases
     */
    @Override
    public void removeAllVertices() {
        //removing content of vert tables 
        for (Object tableName : vertTables.keySet()) {
            String query = String.format("DELETE FROM %s;", tableName.toString());
            executeSQLQuery(query);
        }
        //removing content of edge table
        executeSQLQuery("DELETE FROM Edges;");
    };

    /**
     *  helper function to execute sql queries when return is not reqired
     */
    private boolean executeSQLQuery(String query) {
        boolean success = false;
        try {
            int count = statement.executeUpdate(query);
            success = (count != 0);
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to execute query: " + query);
        }

        return success;
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
        sanityCheck("get edge count", relation, "relation");
        sanityCheck("get edge count", inVertID, "inVertID");
        sanityCheck("get edge count", outVertID, "outVertID");

        int count = 0;
        StringBuilder query = new StringBuilder()
            .append("SELECT COUNT(*) FROM Edges WHERE relation = '")
            .append(relation)
            .append("' AND outVertID = '")
            .append(outVertID)
            .append("' AND inVertID = '")
            .append(inVertID)
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

        return count;
    };
    
    /**
     * 
     * @param property name
     * @param condition 
     * @param value
     */
    @Override
    public DBConstraint getConstraint(String property, Condition condition, Object value) {
        if (condition == Condition.substring) {
            value = new StringBuilder()
                .append("'%")
                .append(value.toString())
                .append("%'")
                .toString();
        } else if (condition == Condition.contains) {
            value = new StringBuilder()
                .append("ARRAY['")
                .append(value.toString())
                .append("']")
                .toString();
        } else {
            value = new StringBuilder()
                .append("'")
                .append(value.toString())
                .append("'")
                .toString();
        }

        return new PostgresqlDBConstraint(property, condition, value);
    };

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
}
