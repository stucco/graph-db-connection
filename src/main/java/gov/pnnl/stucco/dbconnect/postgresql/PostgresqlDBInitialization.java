package gov.pnnl.stucco.dbconnect.postgresql;

import gov.pnnl.stucco.dbconnect.StuccoDBException;
import gov.pnnl.stucco.dbconnect.postgresql.PostgresqlDBPreparedStatement.Columns;

import java.util.Set;
import java.util.HashSet; 
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.PrintWriter;  
import java.io.StringWriter;

import java.sql.Statement;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;

import org.json.JSONObject;
import org.json.JSONArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 

/**
 * @author Maria Vincent
 * Class to initialize Postgresql DB for Stucco graph
 */
public class PostgresqlDBInitialization {

    private static Logger logger = LoggerFactory.getLogger(PostgresqlDBInitialization.class);
    private static JSONObject tables;
    private static String initScript;
    private Statement statement;

    /**
     * constructor reads PostgresqlDBInitialization.sql file, containing 
     * routines to perform Stucco graph alignment on all newly loaded subgraphs with existing graph
     * inside of Postgresql server 
     * @param statement: jdbc statement to execute sql queries
     * @param tables: json config file with all table names, columns, constraints, indexes, etc
     */
    public PostgresqlDBInitialization(Statement statement, JSONObject tables) {
        this.statement = statement;
        this.tables = tables;
        
        try {
            initScript = IOUtils.toString(PostgresqlDBInitialization.class.getClassLoader().getResourceAsStream("postgresql/PostgresqlDBInitialization.sql"), "UTF-8");
        } catch (IOException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e)); 
            e.printStackTrace();
            throw new StuccoDBException("could not initialize PostgreSQL db");
        }
    }

    protected void initDB() {
        try {
            /* 
                loading alignment rules and helper functions into Postgresql; 
                most functions are in PostgresqlDBInitialization.sql file, 
                except insert_vertex(), which is built based on tables.json 
                and passed to Postgeresql seperatley
            */
            statement.executeUpdate(initScript);
            String insertVertices = buildInsertVertices(tables.getJSONObject("vertices"));
            executeSQLUpdate(insertVertices);

            /* building tables based on tables.json config */
            createTables(tables.getJSONObject("vertices"));
            createTables(tables.getJSONObject("edges"));

            /* building indexes; index constraints are in tables.json config */
            buildIndex(null);
        } catch (SQLException e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            throw new StuccoDBException("could not initialize PostgreSQL db (load alignment functions/create tables/build indexes)");
        } 
    }

    /**
     * function to build and execute create table sql  
     * @param tables: json file with all tables info (columns, constraints, indexes, etc)
     */
    private void createTables(JSONObject tables) throws SQLException {    
        for (String tableName : (Set<String>)tables.keySet()) {
            JSONObject table = tables.getJSONObject(tableName);
            String columnsList = buildColumnsList(tableName, table);
            String query = String.format("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, columnsList);
            statement.executeUpdate(query);
        } 
    }

    /**
     * funciton to compose a list of table columns in provided order
     * @param tableName: table name
     * @param table: table columns, their order and constraints
     */
    private String buildColumnsList(String tableName, JSONObject table) {
        String delimiter = "";
        String columnsList = "";
        JSONObject columns = table.getJSONObject("columns");
        JSONArray order = table.getJSONArray("order");
        for (int i = 0; i < order.length(); i++) {
            String columnName = order.getString(i);
            String constraint = columns.getJSONObject(columnName).getString("constraint");
            columnsList = buildString(columnsList, delimiter, columnName, " ", constraint);
            delimiter = ", ";
        }

        return columnsList;
    }

    /**
     * function to build indexes based on tables.config
     * @param indexConfig - is not required for postgresql
     * @throws StuccoDBException 
     */
    public void buildIndex(String filePath) throws SQLException {
        buildIndexHelper(tables.getJSONObject("vertices"));
        buildIndexHelper(tables.getJSONObject("edges"));
    }

    private void buildIndexHelper(JSONObject tables) throws SQLException {
        for (String tableName : (Set<String>)tables.keySet()) {
            JSONObject indexes = tables.getJSONObject(tableName).getJSONObject("indexes");
            JSONArray index = indexes.optJSONArray("UNIQUE");
            if (index != null) {
                String columnsList = jsonArrayToString(index);
                String sql = String.format("SELECT add_unique_constraint('%s', '%s_%s_idx', '%s');", tableName, tableName, columnsList.replaceAll(", ", "_"), columnsList);
                statement.executeQuery(sql);
            }
            index = indexes.optJSONArray("GIN");
            if (index != null) {
                String columnsList = jsonArrayToString(index);
                String sql = String.format("CREATE INDEX IF NOT EXISTS %s_alias_idx ON %s USING gin(%s);", tableName, tableName, columnsList);
                statement.execute(sql);
            }
            index = indexes.optJSONArray("BTREE");
            if (index != null) {
                String columnsList = jsonArrayToString(index);
                String sql = String.format("CREATE INDEX IF NOT EXISTS %s_timestamp_idx ON %s (%s);", tableName, tableName, columnsList);
                statement.execute(sql);
            }
        }
    };

    /**
     * funciton to perform insertion or merging (if duplicated was detected) of vertices based on tables.json
     * @param vertTables: config of all tables including columns, constraints, etc.
     */
    private String buildInsertVertices(JSONObject vertTables) {

        String align = buildString(
            "CREATE OR REPLACE FUNCTION insert_vertex(vertex_id text, vertex json) RETURNS text AS $$ ", 
            "DECLARE id text; duplicate boolean; related_id text; detail text; message text; hint text; ",
            "BEGIN ",  
            "    CASE vertex->>'vertexType' "
        );
        for (String tableName : (Set<String>) vertTables.keySet()) {
            JSONObject table = vertTables.getJSONObject(tableName).getJSONObject("columns");
            JSONArray order = vertTables.getJSONObject(tableName).getJSONArray("order");
            String columns = "_id";
            String values = "vertex_id";
            String delimiter = ", \n";
            List<String> onUpdate = new ArrayList<String>();
            List<String> onUpdateAlias = new ArrayList<String>();
            for (int i = 3; i < order.length(); i++) {
                String columnName = order.getString(i);
                if (table.has(columnName)) {
                    columns = buildString(columns, delimiter, columnName);
                    switch (Columns.valueOf(columnName).type) {
                        case TEXT: 
                            values = buildString(values, delimiter, "vertex->>'", columnName, "'");
                            break;
                        case ARRAY:
                            values = buildString(values, delimiter, "json_to_array((vertex->>'", columnName, "')::json)");
                            String update = buildString(columnName, "=(ARRAY(SELECT DISTINCT unnest(", tableName, ".", columnName, " || EXCLUDED.", columnName, ")))");
                            onUpdate.add(update);
                            update = buildString(columnName, "=(ARRAY(SELECT DISTINCT unnest(", columnName, " || json_to_array((vertex->>'", columnName, "')::json))))");
                            onUpdateAlias.add(update);
                            break;
                        case LONG:
                            values = buildString(values, delimiter, "(vertex->>'", columnName, "')::bigint");
                            break;
                        case UUID:
                            values = buildString(values, delimiter, "(vertex->>'", columnName, "')::uuid");
                            break;
                    }
                }
            }
            if (table.has("alias") && !tableName.equals("Observable")) {
                String update = onUpdateAlias.toString().replaceAll("\\[|\\]", "");
                update = update.replaceAll(", ", ", ");
                String insertSql = buildString(
                    "SELECT _id INTO id FROM " + tableName + " WHERE json_to_array((vertex->>'alias')::json) && alias; ",
                    "IF id IS NULL THEN ",
                    "INSERT INTO ", tableName, " (", columns, ") VALUES (", values, ") RETURNING ", tableName, "._id INTO id; ",
                    "ELSE ", 
                    "UPDATE ", tableName, " SET ", update,
                    "WHERE _id=id; ",
                    // "INSERT INTO duplicates VALUES (vertex_id, id); ",
                    "END IF; "
                ); 
                String whenClauser = buildString("WHEN '", tableName, "' THEN ", insertSql);
                align = buildString(align, whenClauser);

            } else {
                String onConflict = vertTables.getJSONObject(tableName).getJSONObject("indexes").getJSONArray("UNIQUE").toString().replaceAll("\\[|\\]", "");
                // onUpdate.add("_id = duplicateID(vertex_id, " + tableName + "._id)");
                onUpdate.add("modifieddate = now()");
                String update = onUpdate.toString().replaceAll("\\[|\\]", "");
                update = update.replaceAll(", ", ", ");
                String insertSql = buildString(
                    "INSERT INTO ", tableName, " (", columns, ") VALUES (", values, ") ON CONFLICT (", onConflict.toLowerCase(), ") DO UPDATE SET ", update, " RETURNING ", tableName, "._id INTO id; "
                );

                String whenClauser = buildString("WHEN '", tableName, "' THEN  ", insertSql);
                align = buildString(align, whenClauser);
            }

            if (tableName.equals("AddressRange")) {
                align = buildString(
                    align, 
                    " SELECT _id INTO related_id FROM IP WHERE ipInt BETWEEN (vertex->>'startIPInt')::bigint AND (vertex->>'endIPInt')::bigint; ",
                    "IF related_id IS NOT NULL THEN ",
                        "INSERT INTO Edges (outVertID, inVertID, outVertTable, inVertTable, relation)  ",
                        "VALUES (related_id, vertex_id, 'IP', 'AddressRange', 'Contained_Within') ",  
                        "ON CONFLICT (relation, outVertID, inVertID) ",
                        "DO NOTHING; ",
                    "END IF; "
                );
            } else if (tableName.equals("IP")) {
                align = buildString(
                align,
                    " SELECT _id INTO related_id FROM AddressRange WHERE (vertex->>'ipInt')::bigint BETWEEN startIPInt and endIPInt; ",
                    "IF related_id IS NOT NULL THEN ",
                        "INSERT INTO Edges (outVertID, inVertID, outVertTable, inVertTable, relation) ",
                        "VALUES (vertex_id, related_id, 'IP', 'AddressRange', 'Contained_Within') ",
                        "ON CONFLICT (relation, outVertID, inVertID) ",
                        "DO NOTHING; ",
                    "END IF;"
                );
            }
            
        }
        align = buildString(
            align, 
            " ELSE RETURN NULL; END CASE; ",
            "RETURN id;", 
            "EXCEPTION WHEN OTHERS THEN ", 
                "GET STACKED DIAGNOSTICS detail = PG_EXCEPTION_DETAIL, message = MESSAGE_TEXT, hint = PG_EXCEPTION_HINT; ",
                "RAISE NOTICE 'PG_EXCEPTION_DETAIL: %', detail; ",
                "RAISE NOTICE 'MESSAGE_TEXT: %', message; ",
                "RAISE NOTICE 'PG_EXCEPTION_HINT: %', hint; ",
                "RAISE NOTICE 'VERTEX: %', vertex; ",
                "RETURN NULL; ",
            "END; $$ language plpgsql; "
        );

        return align;
    }

    /**
     *  helper function to execute sql queries when return is not reqired
     * @param query: sql query
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

    private static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);

        return sw.toString();
    }

    private static String jsonArrayToString(JSONArray array) {
        StringBuilder str = new StringBuilder();
        String delimiter = "";
        for (int i = 0; i < array.length(); i++) {
            str.append(delimiter);
            str.append(array.get(i));
            delimiter = ", ";
        }

        return str.toString();
    }
}
