package gov.pnnl.stucco.dbconnect;

/**
 * $OPEN_SOURCE_DISCLAIMER$
 *
 */
import java.util.Map;
import java.util.List;

/**
 * DBConnectionAlignment interface to support various connections into the graph DB for alignment.
 *
 */
public interface DBConnectionAlignment {

    /**
     * Retrieves the vertex's property map as referenced by the vertex ID
     * @param id as per the DB
     * @return a property map of the vertex, user must know based on the key how to recast the object type to use its value
     */
    public Map<String, Object> getVertByID(String id);

    /**
     * Retrieve the vertex's property map using a canonical name for the vertex
     * @param name - a canonical name for the vertex node
     * @return a property map of the vertex, user must know based on the key how to recast the object type to use its value
     */
    public Map<String, Object> getVertByName(String name);

    /**
     * Retrieves the vertex ID using the vertex canonical name
     * @param name - canonical name of the vertex
     * @return ID
     */
    public String getVertIDByName(String name);

    /**
     * Given a property map add a new vertex to the DB
     * @param properties - the property map of the vertex
     * @return ID of the vertex as defined in the DB
     */
    public String addVertex(Map<String, Object> properties);

    /**
     * Perform a query/search of the DB using the following constraints on the request
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    public List<String> getVertIDsByConstraints(List<DBConstraint> constraints);

    /**
     * create an edge given two vertex IDs with a specific relationship type
     * @param v1 - vertex ID
     * @param v2 - vertex ID
     * @param relation - relationship type
     * @return edge ID
     */
    public String addEdge(String v1, String v2, String relation);

    /**
     * return the property map of an edge
     * @param id of the edge
     * @return property map
     */
    public Map<String,Object> getEdgeById(String id);

    /**
     * identifies the edges that exist between two vertices with a specific relationship type
     *  (TBD:  DOES ORDER MATTER for v1 and v2?)
     * @param v1 vertex ID for first vertex
     * @param v2 vertex ID for second vertex
     * @param relation type of relationship for the edge
     * @return list of edge IDs that meet the input criteria
     */
    public List<String>getEdgeIDsByVert(String v1, String v2, String relation);

    /**
     * Identify the vertices where their relationship type and direction enter the specified vertex
     * @param v1 vertex end point
     * @param relation the relationship type of the edge
     * @return list of vertex IDs
     */
    public List<String>getInVertIDsByRelation(String v1, String relation);

    /**
     * Identify the vertices where their relationship type and direction leave the specified vertex
     * @param v1 vertex starting point
     * @param relation the relationship type of the edge
     * @return list of vertex IDs
     */
    public List<String>getOutVertIDsByRelation(String v1, String relation);

    /**
     * Identify all vertices where their relationship type and direction either enter or leave the specified vertex
     * @param v1 - vertex ID
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    public List<String>getVertIDsByRelation(String v1, String relation);
    /**
     * Given a specific edge remove it from the DB
     * @param id - edge ID
     * @return property map of the edge
     */
    public Map<String,Object> removeEdgeByID(String id);

    /**
     * Given a specific vertex remove it from the DB
     * (Note any edges connected to it will also be removed)
     * @param id - vertex ID
     * @return property map of the vertex
     */
    public Map<String,Object> removeVertByID(String id);

    // GN: If there's a close, then it's natural to expect an open. If it's
    // assumed it's not needed because the constructor will do that, then the
    // documentation should probably say so. If we use a factory to construct
    // the implementation class, it may be better to have it not open the
    // database while still in the factory, and use an explicit open method
    // instead. One advantage of that would be the caller of open() might be
    // in a better position to handle any exceptions.
    /**
     * Close the DB and commit any transactions
     */
    public void close();





}
