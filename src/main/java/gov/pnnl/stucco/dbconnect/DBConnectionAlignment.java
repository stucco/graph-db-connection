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
     * gets the number of vertices in the graph
     */
    public long getVertCount();
    
    /**
     * gets the number of edges in the graph
     */
    public long getEdgeCount();

    /**
     * Retrieves the vertex's property map as referenced by the vertex ID
     * @param id as per the DB
     * @return a property map of the vertex, user must know based on the key how to recast the object type to use its value
     */
    public Map<String, Object> getVertByID(String id);

    /**
     * Given a property map add a new vertex to the DB
     * @param properties - the property map of the vertex
     * @return ID of the vertex as defined in the DB
     */
    public String addVertex(Map<String, Object> properties);

    /**
     * create an edge given two vertex IDs with a specific relationship type
     * @param inVertID - in vertex ID
     * @param outVertID - out vertex ID
     * @param relation - relationship type
     */
    public void addEdge(String inVertID, String outVertID, String relation);

    /**
     * returns list of edge info maps for the outgoing edges of this vertex
     * @param vertName
     * @return list of edge property maps
     */
    public List<Map<String, Object>> getOutEdges(String outVertID);

    /**
     * returns list of edge info maps for the incoming edges of this vertex
     * @param vertName
     * @return list of edge property maps
     */
    public List<Map<String, Object>> getInEdges(String inVertID);

    /**
     * returns list of edge info maps for the outgoing edges of this vertex
     * @param vertName
     * @return list of edge property maps
     */
    public List<Map<String, Object>> getOutEdgesPage(String outVertID, int offset, int limit);

    /**
     * returns list of edge info maps for the incoming edges of this vertex
     * @param vertName
     * @return list of edge property maps
     */
    public List<Map<String, Object>> getInEdgesPage(String inVertID, int offset, int limit);

    /**
     * Identify the vertices where their relationship type and direction enter the specified vertex
     * @param v1 - vertex end point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    public List<String>getInVertIDsByRelation(String v1, String relation);

    /**
     * Identify the vertices where their relationship type and direction leave the specified vertex
     * @param v1 - vertex starting point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    public List<String>getOutVertIDsByRelation(String v1, String relation);

    /**
     * Identify all vertices where their relationship type and direction either enter or leave the specified vertex
     * @param v1 - vertex starting or ending point
     * @param relation - the relationship type of the edge
     * @return list of vertex IDs
     */
    public List<String>getVertIDsByRelation(String v1, String relation);
    
    /**
     * Identify the vertices where their relationship type and direction enter the specified vertex
     * and where the found vertices match the following constraints
     * @param v1 - vertex ending point
     * @param relation - the relationship type of the edge
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    public List<String> getInVertIDsByRelation(String v1, String relation, List<DBConstraint> constraints);
    
    /**
     * Identify the vertices where their relationship type and direction leave the specified vertex
     * and where the found vertices match the following constraints
     * @param v1 - vertex starting point
     * @param relation - the relationship type of the edge
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    public List<String> getOutVertIDsByRelation(String v1, String relation, List<DBConstraint> constraints);

    /**
     * Identify all vertices where their relationship type and direction either enter or leave the specified vertex
     * and where the found vertices match the following constraints
     * @param v1 - vertex starting or ending point
     * @param relation - the relationship type of the edge
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    public List<String> getVertIDsByRelation(String v1, String relation, List<DBConstraint> constraints);
    
    /**
     * Perform a query/search of the DB using the following constraints on the request
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    public List<String> getVertIDsByConstraints(List<DBConstraint> constraints);

    /**
     * Perform a query/search of the DB using the following constraints on the request
     * @param constraints - list of constraint objects
     * @return list of vertex IDs
     */
    public List<String> getVertIDsByConstraints(List<DBConstraint> constraints, int offcet, int limit);
    
    /**
     * Given two vertices and a relation, remove the edge
     * @param inVertID - in vertex ID
     * @param outVertID - out vertex ID
     * @param relation - relationship type
     */
    public void removeEdgeByRelation(String inVertID, String outVertID, String relation);

    /**
     * Given a specific vertex remove it from the DB
     * (Note any edges connected to it will also be removed)
     * @param id - vertex ID
     */
    public void removeVertByID(String id);
    
    /**
     * Replaces current vertex's property map with a different one
     * @param id - vertex that will be changed
     * @param properties - property map with different contents, complete replacement of current content
     */
    public void updateVertex(String id, Map<String, Object> properties);

    /**
     * Close the DB and commit any transactions, for certain system it may be a NO-OP
     */
    public void close();
    
    /**
     * Open the DB prior to working with the system, for every open there should be a 
     * corresponding close()
     * If the system is already open() not other connections will be made from this thread
     */
    public void open();

    /**
     * get the number of edges based on incoming vertex and outgoing vertex and a relationship
     * @param inVertID - incoming vertex
     * @param outVertID - outgoing vertex
     * @param relation - label/relations on the edge
     * @return a count of edges
     */
    public int getEdgeCountByRelation(String inVertID, String outVertID, String relation);
    
    /**
     * 
     * @param property name
     * @param condition 
     * @param value
     */
    public DBConstraint getConstraint(String property, Condition condition, Object value);
    



}
