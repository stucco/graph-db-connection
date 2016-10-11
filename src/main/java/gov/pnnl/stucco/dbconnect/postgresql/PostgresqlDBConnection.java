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
import java.sql.DatabaseMetaData;    
import java.sql.ResultSet;   
import java.sql.SQLException;   
import java.sql.Array;
import java.sql.Timestamp;  
import java.sql.Types;

import org.postgresql.util.PGobject;
    
import java.io.PrintWriter;  
import java.io.StringWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream; 
import java.io.OutputStream;
import java.io.PrintStream;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet; 
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays; 
import java.util.Collection;
import java.util.UUID;

import org.apache.commons.io.IOUtils;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

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
    private PostgresqlDBPreparedStatement ps;
    private PGobject uuid;

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
        uuid = new PGobject();
        uuid.setType("uuid");
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
        //PostgreSQL connecting url has following form: jdbc:postgresql://hostname:port/database
        String hostname = configuration.get("hostname").toString();
        String port = configuration.get("port").toString();
        String dbName = configuration.get("database").toString();
        String url = buildString("jdbc:postgresql://", hostname, ":", port, "/", dbName);
        String username = (configuration.containsKey("username")) ? configuration.get("username").toString() : null;
        String password = (configuration.containsKey("password")) ? configuration.get("password").toString() : null;
        try {
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement(); 
            ps = new PostgresqlDBPreparedStatement(connection, vertTables);
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("could not create PostgreSQL client connection");
        }
 
        try {
            createTables();
            buildIndex(null);
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("could not create PostgreSQL tables");
        } 
    };

    private void createTables() throws SQLException {
        statement.executeUpdate("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";");
        //creating tables for vertices
        for (Object key : vertTables.keySet()) {
            String tableName = key.toString();
            JSONObject table = vertTables.getJSONObject(tableName);
            String query = buildCreateTableSQL(tableName, table);
            statement.executeUpdate(query);
        } 
        //creating tables for edges
        boolean enum_exists = selectExists("SELECT EXISTS (SELECT * FROM pg_type WHERE typname = 'tableenum');");
        if (!enum_exists) {
            statement.executeUpdate("CREATE TYPE tableenum AS ENUM " + 
                "('AddressRange', 'Campaign', 'Course_Of_Action', 'Exploit', 'Exploit_Target', " +
                "'Incident', 'Indicator', 'IP', 'Malware', 'Observable', 'Threat_Actor', 'TTP', 'Vulnerability', 'Weakness');");
        }
        String query = ("CREATE TABLE IF NOT EXISTS Edges (timestamp timestamp with time zone not null default now(), relation text NOT NULL, outVertID uuid NOT NULL, inVertID uuid NOT NULL, outVertTable tableEnum  NOT NULL, inVertTable tableEnum NOT NULL);");
        statement.executeUpdate(query);
    }

    private boolean selectExists(String query) throws SQLException {
        boolean exists = false;
        ResultSet rs = statement.executeQuery(query);
        if (rs.next()) {
            exists = rs.getBoolean(1);
        }

        return exists;
    }

    private String buildCreateTableSQL(String tableName, JSONObject table) {
        String delimiter = "";
        String query = buildString(
            "CREATE TABLE IF NOT EXISTS ", 
            tableName, 
            " (_id uuid PRIMARY KEY DEFAULT uuid_generate_v4() NOT NULL UNIQUE, ",
            "timestamp timestamp with time zone DEFAULT now() NOT NULL, ",
            "modifieddate timestamp with time zone DEFAULT now() NOT NULL, "
        );
        for (String columnName : columnOrder) {
            if (table.has(columnName)) {
                String constraint = table.getJSONObject(columnName).getString("constraint");
                query = buildString(query, delimiter, columnName, " ", constraint);
                delimiter = ", ";
            }
        }
        query = buildString(query, ");");
        
        return query;
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
            ps.close();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("could not close PostgreSQL client connection");
        }
    };

    /**
     * Builds the index for full text search of sourceDocument
     * @param indexConfig - is not required for postgresql
     * @throws StuccoDBException 
     */
    @Override
    public void buildIndex(String filePath) throws StuccoDBException {
        try {
            String query = buildString("CREATE INDEX IF NOT EXISTS ip_ipint_index ON IP (ipInt);");
            statement.executeUpdate(query);
            query = buildString("CREATE INDEX IF NOT EXISTS addressrange_startipint_index ON AddressRange (startIPInt);");
            statement.executeUpdate(query);
            query = buildString("CREATE INDEX IF NOT EXISTS addressrange_endipint_index ON AddressRange (endIPInt);");
            statement.executeUpdate(query);
            query = buildString("CREATE INDEX IF NOT EXISTS edges_outvertid_index ON Edges USING HASH (outVertID);");
            statement.executeUpdate(query);
            query = buildString("CREATE INDEX IF NOT EXISTS edges_invertid_index ON Edges USING HASH (inVertID);");
            statement.executeUpdate(query);
        } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
                throw new StuccoDBException("failed to add indexes");
        }

        for (Object table : vertTables.keySet()) {
            try {
                //creating index on name (b-tree for now, since some names could be the same, such as network flow and network connection both contains src and dest addresses as their names)
                String tableName = table.toString();
                String query = buildString("CREATE INDEX IF NOT EXISTS ", tableName + "_name_index", " ON ", tableName, " (name);");
                statement.executeUpdate(query);
            
                JSONObject vertTable = vertTables.getJSONObject(tableName);
                if (vertTable.has("alias")) {
                    DatabaseMetaData databaseMetaData = connection.getMetaData();
                    ResultSet rs = databaseMetaData.getColumns(null, null,  tableName.toLowerCase(), "tsv");
                    boolean columnExists = rs.next();
                    //creating a column for tsvector (tsv)
                    if (!columnExists) {
                        query = buildString("ALTER TABLE ", tableName, " ADD COLUMN tsv tsvector;");
                        statement.executeUpdate(query);
                        // uncomment to set up gin on description and sourceDocument (need to alter if statement above also ....)
                        //query = buildString("UPDATE ", tableName, " SET tsv = to_tsvector('english', coalesce(sourceDocument, ' ') || ' ' || coalesce(array_to_string(description, ' '), ' '));");
                        query = buildString("UPDATE ", tableName, " SET tsv = to_tsvector('english', coalesce(array_to_string(alias, ' '), ' '));");
                        statement.executeUpdate(query);
                        //setting index on tsv
                        query = buildString("CREATE INDEX CONCURRENTLY ", tableName, "_tsv_idx ON ", tableName, " USING gin(tsv) WITH (fastupdate = on);");
                        statement.executeUpdate(query);
                    }
                    //creating function and a trigger to update tsv every time alian (sourceDocument) is updated
                    boolean functionExists = executeSQLQuery("SELECT routine_name FROM information_schema.routines where routine_name = 'search_trigger';");
                    if (!functionExists) {
                        query = "CREATE FUNCTION search_trigger() RETURNS TRIGGER AS $search_trigger$ " +
                            "BEGIN " +
                            //    "new.tsv := to_tsvector('english', coalesce(array_to_string(new.description, ' '), ' ')) || to_tsvector('english', coalesce(new.sourceDocument, ' ')); " +
                                "new.tsv := to_tsvector('english', coalesce(array_to_string(new.alias, ' '), ' ')); " +
                                "return new; " +
                            "END " +
                            "$search_trigger$ LANGUAGE plpgsql;";
                        statement.executeUpdate(query);
                    }
                    query = buildString("SELECT event_object_table FROM information_schema.triggers WHERE trigger_name = 'tsvectorupdate' AND event_object_table = '", tableName.toLowerCase(), "';");
                    boolean triggerExists = executeSQLQuery(query);
                    if (!triggerExists) {
                        query = buildString("CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE ON ", tableName, " FOR EACH ROW EXECUTE PROCEDURE search_trigger();");
                        statement.executeUpdate(query);
                    }
                }
                /*
                //creating index on vertexType
                query = buildString("CREATE INDEX IF NOT EXISTS ", tableName + "_vertextype_index", " ON ", tableName, " (vertexType);");
                statement.executeUpdate(query);
                //creating index on observableType
                JSONObject vertTable = vertTables.getJSONObject(tableName);
                if (vertTable.has("observableType")) {
                    query = buildString("CREATE INDEX IF NOT EXISTS ", tableName + "_observabletype_index", " ON ", tableName, " (observableType);");
                    statement.executeUpdate(query);
                }
                */
            } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
                throw new StuccoDBException("failed to add indexes");
            }
        }
    };

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
            JSONObject table = vertTables.getJSONObject(tableName);
            for (Object key : table.keySet()) {
                String column = key.toString();
                switch (Columns.valueOf(column).type) {
                    case TEXT: 
                        if (properties.containsKey(column)) {
                            preparedStatement.setString(Columns.valueOf(column).index, properties.get(column).toString());
                        } else {
                            preparedStatement.setNull(Columns.valueOf(column).index, Types.VARCHAR);
                        }
                        break;
                    case ARRAY:
                        if (properties.containsKey(column)) {
                            Collection collection = (Collection) properties.get(column);
                            Object[] array = (Object[]) collection.toArray();
                            Array sqlArray = connection.createArrayOf("text", array);
                            preparedStatement.setArray(Columns.valueOf(column).index, sqlArray);
                        } else {
                            preparedStatement.setNull(Columns.valueOf(column).index, Types.ARRAY);
                        }
                        break;
                    case LONG:
                        if (properties.containsKey(column)) {
                            preparedStatement.setLong(Columns.valueOf(column).index, ((Number)properties.get(column)).longValue());
                        } else {
                            preparedStatement.setNull(Columns.valueOf(column).index, Types.BIGINT);
                        }
                        break;
                    case TIMESTAMP:
                        if (properties.containsKey(column)) {
                            preparedStatement.setTimestamp(Columns.valueOf(column).index, (Timestamp)properties.get(column));
                        } else {
                            preparedStatement.setNull(Columns.valueOf(column).index, Types.TIMESTAMP);
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
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to add new vertex with name - " + properties.get("name") + ", and vertexType - " + properties.get("vertexType"));
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
        for (Object key : vertTables.keySet()) {
            String tableName = key.toString();
            PreparedStatement preparedStatement = ps.getPreparedStatement(tableName, API.GET_VERT_BY_ID);
            try {
                uuid.setValue(id);
                preparedStatement.setObject(1, uuid);
                ResultSet rs = preparedStatement.executeQuery();
                if (rs.next()) {
                    vertex = vertResultSetToMap(tableName, rs);
                    break;
                }
            } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
                throw new StuccoDBException("failed to get vertex by id");
            }
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
            preparedStatement.setObject(2, UUID.fromString(outVertID));
            preparedStatement.setObject(3, UUID.fromString(inVertID));
            preparedStatement.setString(4, outVertTable);
            preparedStatement.setString(5, inVertTable);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to add edge!");
        }  
    };

    private String getVertexType(String vertID) throws SQLException {
        String vertexType = null;
        for (Object table : vertTables.keySet()) {
            String tableName = table.toString();
            PreparedStatement preparedStatement = ps.getPreparedStatement(tableName, API.GET_VERTEX_TYPE_BY_ID);
            preparedStatement.setObject(1, UUID.fromString(vertID));
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
            preparedStatement.setObject(1, UUID.fromString(outVertID));
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                edges.add(map);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to get out edges!");
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
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_OUT_EDGES_PAGE);
        try {
            preparedStatement.setObject(1, UUID.fromString(outVertID));
            preparedStatement.setInt(2, offset);
            preparedStatement.setInt(3, limit);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                edges.add(map);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to get out edges!");
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
            preparedStatement.setObject(1, UUID.fromString(inVertID));
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                edges.add(map);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to get in edges!");
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
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_IN_EDGES_PAGE);

        try {
            preparedStatement.setObject(1, UUID.fromString(inVertID));
            preparedStatement.setInt(2, offset);
            preparedStatement.setInt(3, limit);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                Map<String, Object> map = edgesResultSetToMap(rs);
                edges.add(map);
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to get in edges!");
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
            throw new StuccoDBException("failed to select edges with query: " + query);
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
            preparedStatement.setObject(2, UUID.fromString(outVertID));
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                vertIDs.add(rs.getString("vertID"));
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to get in vert ids!");
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
            preparedStatement.setObject(2, UUID.fromString(outVertID));
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                UUID vertID = (UUID) rs.getObject(1);
                String tableName = rs.getString(2);
                String query = buildString("SELECT _id AS vertID FROM ", tableName, " WHERE _id = '", vertID, "' AND ", constraintsList, ";");
                vertIDs.addAll(getVertIDs(query));   
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to get in vert ids by relation and constraints!");
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
            preparedStatement.setObject(2, UUID.fromString(inVertID));
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                vertIDs.add(rs.getString("vertID"));
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to get out vert ids!");
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
            preparedStatement.setObject(2, UUID.fromString(inVertID));
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                UUID vertID = (UUID) rs.getObject(1);
                String tableName = rs.getString(2);
                String query = buildString("SELECT _id AS vertID FROM ", tableName, " WHERE _id = '", vertID, "' AND ", constraintsList, ";");
                vertIDs.addAll(getVertIDs(query));   
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to get out vert ids by relation and constraints!");
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
            preparedStatement.setObject(2, UUID.fromString(vertID));
            preparedStatement.setString(3, relation);
            preparedStatement.setObject(4, UUID.fromString(vertID));
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                vertIDs.add(rs.getString("vertID"));
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to get vert ids!");
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
                    String query = buildString("SELECT _id as vertID FROM ", tableName, " WHERE ", constraintsList, " order by timestamp desc offset ", offset, " LIMIT ", limit);
                    vertIDs.addAll(getVertIDs(query));
                    break;
                }
            }
        } else {
            for (Object table : vertTables.keySet()) {
                String tableName = table.toString();
                if (containsAllColumns(tableName, columnList)) {
                    String query = buildString("SELECT _id as vertID FROM ", tableName, " WHERE ", constraintsList, " order by timestamp desc offset ", offset, " LIMIT ", limit);
                    vertIDs.addAll(getVertIDs(query));
                }
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

    private boolean containsAllColumns(String tableName, List<String> columnNames) {
        Set tableColumns = vertTables.getJSONObject(tableName).keySet();
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
            preparedStatement.setObject(2, UUID.fromString(outVertID));
            preparedStatement.setObject(3, UUID.fromString(inVertID));
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to remove edge!");
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
                preparedStatement.setObject(1, UUID.fromString(vertID));
                int count = preparedStatement.executeUpdate();
                if (count != 0) {
                    removeEdgeByVertID(vertID);
                    break;
                }
            } catch (SQLException e) {
                logger.warn(e.getLocalizedMessage());
                logger.warn(getStackTrace(e));
                throw new StuccoDBException("Failed to remove vert by id!");
            }
        }

    };

    /**
     * remove edge if it contains outVertID or inVertID matching vertID
     */
    private void removeEdgeByVertID(String vertID) {
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.REMOVE_EDGE_BY_VERT_ID);
        try {
            uuid.setValue(vertID);
            preparedStatement.setObject(1, uuid);
            preparedStatement.setObject(2, uuid);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("Failed to remove edge by vert ID!");
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
            throw new StuccoDBException("Failed to remove all vertices!");            
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
                throw new StuccoDBException("failed to select all vertices from DB");
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
            throw new StuccoDBException("failed to get all edges from DB!");
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
                throw new StuccoDBException("failed to count rows in table: " + tableName);
            }
        }
        
        return count;
    };
    
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
        PreparedStatement preparedStatement = ps.getPreparedStatement("Edges", API.GET_EDGE_COUNT_BY_RELATION);
        try {
            preparedStatement.setString(1, relation);
            preparedStatement.setObject(2, UUID.fromString(outVertID));
            preparedStatement.setObject(3, UUID.fromString(inVertID));
            ResultSet rs = preparedStatement.executeQuery();
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
     * helper funciton to collect list of ids selected by query
     * @param query - select Edge table query with some constraints
     * @return list of vert ids selected by query
     */
    private List<String> getVertIDs(String query) {
        List<String> vertIDs = new ArrayList<String>();
        try {
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                vertIDs.add(rs.getString("vertID"));
            }
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("failed to select vertIDs with query: " + query);
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
            throw new StuccoDBException("failed to execute query: " + query);
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
            throw new StuccoDBException("failed to execute query: " + query);
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
        if (condition == Condition.substring) {
            value = buildString("'%", value, "%'");
        } else if (condition == Condition.contains) {
            value = buildString("ARRAY['", value, "']");
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
        JSONObject table = vertTables.getJSONObject(tableName);
        for (Object column : table.keySet()) {
            String columnName = column.toString();
            if (Columns.valueOf(columnName).type == TYPE.ARRAY) {
                Array array = rs.getArray(columnName);
                if (array != null) {
                    Object value = new HashSet<Object>(Arrays.asList((Object[]) array.getArray()));
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
                case LONG:
                    value = ((Number)value).longValue();
                    break;
            }
            vert.put(column, value);
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
}
