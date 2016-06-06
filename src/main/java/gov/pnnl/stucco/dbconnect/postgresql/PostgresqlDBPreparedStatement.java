package gov.pnnl.stucco.dbconnect.postgresql;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement; 
import java.sql.SQLException;
 
import java.util.Map;
import java.util.HashMap;  
import java.util.Arrays; 
import java.util.Comparator;

import org.apache.commons.lang3.StringUtils;

import org.json.JSONObject;

public class PostgresqlDBPreparedStatement {

	private static JSONObject vertTables;
	private Connection connection;
	private Map<String, Map<API, PreparedStatement>> preparedStatements;

	/**
	 * comparator to sort and insert keys into specified positions in PreparedStatements
	 */
	public static class ColumnIndexComparator implements Comparator<String> {
		public int compare(String c1, String c2) {
			int c1index = Columns.valueOf(c1).index;
			int c2index = Columns.valueOf(c2).index;

			return c1index - c2index;
		}
	}

	/**
	 * type field for api enum
	 * used during population of PreparedStatement 
	 */
	private enum TABLE {
		VERTEX_TABLE, EDGE_TABLE;
	}

	/**
	 * useful during insertion into PreparedStatement 
	 * TYPE.TEXT -> setString()
	 * TYPE.ARRAY -> setArray()
	 * TYPE.BIGINT -> setLong() ... there is no setBigint() function
	 * TYPE.TIMESTAMP -> setTimestamp()
	 * etc...
	 */
	public enum TYPE {
		TEXT, ARRAY, BIGINT, TIMESTAMP;
	}

	/**
	 * Columns enumerates position of every field in PreperedStatement,
	 * determines type for insertion into PreperedStatement
	 */
	public enum Columns {
		_id (0, TYPE.TEXT),
		name (1, TYPE.TEXT),
		vertexType (2, TYPE.TEXT),
		description (3, TYPE.ARRAY),
		sourceDocument (4, TYPE.TEXT),
		source (5, TYPE.ARRAY),
		observableType (6, TYPE.TEXT),
		shortDescription(6, TYPE.ARRAY),
		alias (7, TYPE.ARRAY),
		publishedDate (7, TYPE.TIMESTAMP),
		ipInt (7, TYPE.BIGINT),
		location (7, TYPE.TEXT),
		details (8, TYPE.ARRAY),
		startIP (8, TYPE.TEXT),
		endIP (9, TYPE.TEXT),
		startIPInt (10, TYPE.BIGINT),
		endIPInt (11, TYPE.BIGINT);

		public int index;
		public TYPE type;
		private Columns(int index, TYPE type) {
			this.index = index;
			this.type = type;
		}
	}

	/**
	 * API enum return sql statements to generate PreparedStatements;
	 * API enum is missing 5 api functions: 
	 * 	- getInVertIDsByRelation(String outVertID, String relation, List<DBConstraint> constraints)
	 *	- getOutVertIDsByRelation(String inVertID, String relation, List<DBConstraint> constraints)
	 *	- getVertIDsByRelation(String vertID, String relation, List<DBConstraint> constraints)
	 *	- getVertIDsByConstraints(List<DBConstraint> constraints)
	 *	- updateVertex(String vertID, Map<String, Object> properties)
	 * since all of them come with a list/map of constraints/properties of unkown length, precompilation is not possible
	 * enum is missing 1 api function:
	 *	- setPropertyInDB(String vertID, String key, Object newValue)
	 * since property name (key) is unknown at compile time
	 */
	public enum API {
		ADD_VERTEX ("addVertex", TABLE.VERTEX_TABLE) {
			public String getStatement(String... args) {
				return String.format("INSERT INTO %s (%s) VALUES (%s);", args[0], args[1], args[2]);
			}
		},
		GET_VERT_BY_ID ("getVertByID", TABLE.VERTEX_TABLE) {
			public String getStatement(String... args) { 
				return String.format("SELECT * FROM %s WHERE _id = ?;", args[0]); 
			}
		},
		GET_VERT_COUNT ("getVertCount", TABLE.VERTEX_TABLE) {
			public String getStatement(String... args) { 
				return String.format("SELECT COUNT(*) FROM %s;", args[0]); 
			}
		},
		GET_ALL_VERTICES ("getAllVertices", TABLE.VERTEX_TABLE) {
			public String getStatement(String... args) { 
				return String.format("SELECT * FROM %s;", args[0]); 
			}
		},
		REMOVE_ALL_VERTICES ("removeAllVertices", TABLE.VERTEX_TABLE) {
			public String getStatement(String... args) { 
				return String.format("DELETE FROM %s;", args[0]); 
			}
		},
		REMOVE_VERT_BY_ID ("removeVertByID", TABLE.VERTEX_TABLE) {
			public String getStatement(String... args) { 
				return String.format("DELETE FROM %s WHERE _id = ?;", args[0]); 
			}
		},
		SET_PROPERTY_IN_DB ("setPropertyInDB", TABLE.VERTEX_TABLE) {
			public String getStatement(String... args) {
				return String.format("UPDATE %s SET ? = ? WHERE _id = ?;", args[0]);
			}
		},
		ADD_EDGE ("addEdges", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "INSERT INTO Edges (relation, outVertID, inVertID, outVertTable, inVertTable) VALUES (?, ?, ?, CAST(? AS tableenum), CAST(? AS tableenum));";
			}
		},
		GET_OUT_EDGES ("getOutEdges", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "SELECT * FROM Edges WHERE outVertID = ?;";
			}
		},
		GET_IN_EDGES ("getInEdges", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "SELECT * FROM Edges WHERE inVertID = ?;";
			}
		},
		GET_IN_VERT_IDS_BY_RELATION ("getInVertIDsByRelation", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "SELECT inVertID AS vertID FROM Edges WHERE relation = ? AND outVertID = ?;";
			}
		},
		GET_OUT_VERT_IDS_BY_RELATION ("getOutVertIDsByRelation", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "SELECT inVertID AS vertID FROM Edges WHERE relation = ? AND outVertID = ?;";
			}
		},
		GET_IN_VERT_IDS_AND_TABLE_BY_RELATION ("getInVertIDsAndTableByRelation", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "SELECT inVertID AS vertID, inVertTable AS tableName FROM Edges WHERE relation = ? AND outVertID = ?;";
			}
		},
		GET_OUT_VERT_IDS_AND_TABLE_BY_RELATION ("getOutVertIDsAndTableByRelation", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "SELECT outVertID AS vertID, outVertTable as tableName FROM Edges WHERE relation = ? AND inVertID = ?;";
			}
		},
		GET_VERT_IDS_BY_RELATION ("getVertIDsByRelation", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return	"SELECT outVertID as vertID FROM Edges WHERE relation = ? AND inVertID = ? " +
								"UNION " +
								"SELECT inVertID as vertID FROM Edges WHERE relation = ? AND outVertID = ?;";
			}
		},
		REMOVE_EDGE_BY_RELATION ("removeEdgeByRelation", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "DELETE FROM Edges WHERE relation = ? AND outVertID = ? AND inVertID = ?;";
			}
		},
		REMOVE_EDGE_BY_VERT_ID ("removeEdgeByVertID", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "DELETE FROM Edges WHERE outVertID = ? OR inVertID = ?;";
			}
		},
		REMOVE_ALL_EDGES ("removeAllEdges", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return String.format("DELETE FROM Edges;"); 
			}
		},
		GET_ALL_EDGES ("getAllEdges", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "SELECT * FROM Edges;";
			}
		},
		GET_EDGE_COUNT ("getEdgeCount", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "SELECT COUNT(*) FROM Edges";
			}
		},
		GET_EDGE_COUNT_BY_RELATION ("getEdgeCountByRelation", TABLE.EDGE_TABLE) {
			public String getStatement(String... args) {
				return "SELECT COUNT(*) FROM Edges WHERE relation = ? AND outVertID = ? AND inVertID = ?;";
			}
		},
		GET_VERTEX_TYPE_BY_ID ("getVertexTypeByID", TABLE.VERTEX_TABLE) {
			public String getStatement(String... args) {
				return String.format("SELECT vertexType FROM %s WHERE _id = ?;", args[0]);
			}
		};

		public String function;
		public TABLE table;
		public abstract String getStatement(String... args);
		private API (String function, TABLE table) {
			this.function = function;
			this.table = table;
		}
	}

	/**
	 * class construcctor; initialing population of PreparedStatements
	 * @param connection - jdbc connection to instantiate PreparedStatements
	 * @param vertTable - contains names of all vertex tables required to create PreparedStatements 
	 */
	public PostgresqlDBPreparedStatement(Connection connection, JSONObject vertTables) {
		this.connection = connection;
		this.vertTables = vertTables;
		preparedStatements = new HashMap<String, Map<API, PreparedStatement>>();
		try {
			populatePreparedStatementsMap();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * function populates PreparedStatements for every table and it's api
	 */
	private void populatePreparedStatementsMap() throws SQLException {
		for (API api : API.values()) {
			switch (api.table) {
				case VERTEX_TABLE :
					if (api.function.equals("addVertex")) {
						for (Object table : vertTables.keySet()) {
							String tableName = table.toString();
							JSONObject json = vertTables.getJSONObject(tableName);
							String columnNames = getColumnNames(json);
							String values = StringUtils.repeat("?", ", ", json.length());
							String statement = api.getStatement(tableName, columnNames, values);
							Map<API, PreparedStatement> map = new HashMap<API, PreparedStatement>();
							map.put(api, connection.prepareStatement(statement,  Statement.RETURN_GENERATED_KEYS));
							preparedStatements.put(tableName, map);
						}	
					} else {
						for (Object table : vertTables.keySet()) {
							String tableName = table.toString();
							String statement = api.getStatement(tableName);
							Map<API, PreparedStatement> map = (preparedStatements.containsKey(tableName)) ? preparedStatements.get(tableName) : new HashMap<API, PreparedStatement>();
							map.put(api, connection.prepareStatement(statement));
							preparedStatements.put(tableName, map);
						}
					}
					break;
				case EDGE_TABLE :
					String statement = api.getStatement();
					Map<API, PreparedStatement> map = (preparedStatements.containsKey("Edges")) ? preparedStatements.get("Edges") : new HashMap<API, PreparedStatement>();
					map.put(api, connection.prepareStatement(statement));
					preparedStatements.put("Edges", map);
					break;
			}
		}
	}


	private static String getColumnNames(JSONObject columns) {
		String[] columnNames = JSONObject.getNames(columns);
		Arrays.sort(columnNames, new ColumnIndexComparator());

		return arrayToString(columnNames);
	}

	private static String arrayToString(String[] array) {
		StringBuilder sb = new StringBuilder();
		String delimiter = "";
		for (String str : array) {
			sb.append(delimiter);
			sb.append(str);
			delimiter = ", ";
		}

		return sb.toString();
	}

	/**
	 * return PreparedStatement for specified table and function
	 * @param table - name of quered table
	 * @param function - api function name (query)
	 */
	public PreparedStatement getPreparedStatement (String table, API function) {
		return preparedStatements.get(table).get(function);
	}

	/**
	 * closing resources
	 */
	public void close() throws SQLException {
		for (String table : preparedStatements.keySet()) {
			Map<API, PreparedStatement> map = preparedStatements.get(table);
			for (API function : map.keySet()) {
				PreparedStatement ps = map.get(function);
				ps.close();
			}
		}
	}
}