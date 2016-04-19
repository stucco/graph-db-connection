package gov.pnnl.stucco.dbconnect.inmemory;


import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConnectionBase;
import gov.pnnl.stucco.dbconnect.DBConstraint;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;

/**
 * This class represents a concrete implementation of an in-memory DB Connection Type
 *
 */
public class InMemoryDBConnection extends DBConnectionBase{

    /** logger variable to track activities in this class*/
    private Logger logger = null;

    /** contains all the vertices via a map of maps where the values are a property map*/
    private Map<String, Map<String, Object>> vertices = null;

    /** contains a mapping of vertexIDs to the actual vertex canonical name*/
    private Map<String, String> vertIDs = null;

    /** 
     * contains a map of edges and their properties.
     * 
     * <p> Note: We're keeping this structure even though edge IDs are no
     * longer exposed in the interface, in order to minimize code changes.
     */
    private Map<String, Map<String, Object>> edges = null; //TODO: make/use an Edge class, to store inV, outV, label?  And maybe index that.

    //private Map<String, String> edgeIDs = null; //edges don't have meaningful names.
    /** which property names are indexed*/
    private Set<String> indexedVertFields = null; //TODO: not maintaining any indexed fields for now, add later if desired.

    /**
     * Constructor of an InMemory type of DB Connection
     */
    public InMemoryDBConnection(){
        vertices = new HashMap<String, Map<String, Object>>();
        vertIDs = new HashMap<String, String>();
        edges = new HashMap<String, Map<String, Object>>();
        //edgeIDs = new HashMap<String, String>(); //edges don't have meaningful names.
        indexedVertFields = new HashSet<String>();
        //TODO: initialize any indexes.
    }

    @Override
    public long getVertCount(){
        return vertices.size();
    }

    @Override
    public long getEdgeCount(){
        return edges.size();
    }
    
    
    @Override
    public List<Map<String, Object>> getOutEdges(String outVertID) {
        if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
            throw new IllegalArgumentException("cannot get edge with missing or invalid outVertID");
        }
        List<Map<String, Object>> foundEdges = new LinkedList<Map<String, Object>>();
        for(Map<String, Object> currEdge : edges.values()){
            if( ((String)currEdge.get("outVertID")).equals(outVertID) ){
                //inVertID = currEdge.get("inVertID");
                //outVertID = currEdge.get("outVertID");
                //relation = currEdge.get("relation");
                foundEdges.add(currEdge);
            }
        }
        return foundEdges;
    }

    @Override
    public List<Map<String, Object>> getInEdges(String inVertID) {
        if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
            throw new IllegalArgumentException("cannot get edge with missing or invalid inVertID");
        }
        List<Map<String, Object>> foundEdges = new LinkedList<Map<String, Object>>();
        for(Map<String, Object> currEdge : edges.values()){
            if( ((String)currEdge.get("inVertID")).equals(inVertID) ){
                foundEdges.add( currEdge );
            }
        }
        return foundEdges;
    }
    
    /**
     * return the vertex's property map given the vertex ID
     * @param vertID
     * @return property map
     */
    public Map<String, Object> getVertByID(String vertID){
        return vertices.get(vertID);
    }

    /**
     * get the vertex's property map using the vertex's canonical name
     * @param vertName
     * @return property map
     */
    public Map<String,Object> getVertByName(String vertName) {
        if(vertName == null || vertName == "")
            return null;
        String id = vertIDs.get(vertName);
        if(id == null)
            return null;
        Map<String, Object> retVal = vertices.get(id);
        if(retVal == null)
            throw new IllegalStateException("bad state: known vertex name has no known content.");
        return retVal;
    }

    /**
     * get the vertexID using the canonical name
     * @param vertName
     * @return ID
     */
    public String getVertIDByName(String vertName){
        if(vertName == null || vertName == "")
            return null;
        String id = vertIDs.get(vertName);
        return id;
    }


    /**
     * return a list of Incoming vertices based on their edge type relation
     * @param outVertID
     * @param relation
     * @return list of vertices
     */
    public List<String> getInVertIDsByRelation(String outVertID, String relation){
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid relation");
        }
        if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
            throw new IllegalArgumentException("cannot get edge with missing or invalid outVertID");
        }

        List<String> relatedIDs = new LinkedList<String>();
        for(Map<String, Object> currEdge : edges.values()){
            if( ((String)currEdge.get("relation")).equals(relation) ){
                if( ((String)currEdge.get("outVertID")).equals(outVertID) ){
                    relatedIDs.add( (String)currEdge.get("inVertID") ); //TODO: check valid state here?
                }
            }
        }
        return relatedIDs;
    }

    /**
     * return a list of Outgoing vertices based on their edge type relation
     * @param inVertID
     * @param relation
     * @return list of vertices
     */
    public List<String> getOutVertIDsByRelation(String inVertID, String relation){
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid relation");
        }
        if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
            throw new IllegalArgumentException("cannot get edge with missing or invalid inVertID");
        }

        List<String> relatedIDs = new LinkedList<String>();
        for(Map<String, Object> currEdge : edges.values()){
            if( ((String)currEdge.get("relation")).equals(relation) ){
                if( ((String)currEdge.get("inVertID")).equals(inVertID) ){
                    relatedIDs.add( (String)currEdge.get("outVertID") ); //TODO: check valid state here?
                }
            }
        }
        return relatedIDs;
    }

    /**
     * get the list of incoming or outgoing vertices based on edge relationship
     * @param vertID
     * @param relation
     * @return list of vertices
     */
    public List<String> getVertIDsByRelation(String vertID, String relation){
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid relation");
        }
        if(vertID == null || vertID.equals("") || !vertices.containsKey(vertID)){
            throw new IllegalArgumentException("cannot get edge with missing or invalid inVertID");
        }

        List<String> relatedIDs = new LinkedList<String>();
        for(Map<String, Object> currEdge : edges.values()){
            if( ((String)currEdge.get("relation")).equals(relation) ){
                if( ((String)currEdge.get("inVertID")).equals(vertID) ){
                    relatedIDs.add( (String)currEdge.get("outVertID") ); //TODO: check valid state here?
                }else if( ((String)currEdge.get("outVertID")).equals(vertID) ){
                    relatedIDs.add( (String)currEdge.get("inVertID") ); //TODO: check valid state here?
                }
            }
        }
        return relatedIDs;
    }

    @Override
    public int getEdgeCountByRelation(String inVertID, String outVertID, String relation){
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid relation");
        }
        if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
            throw new IllegalArgumentException("cannot get edge with missing or invalid inVertID");
        }
        if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
            throw new IllegalArgumentException("cannot get edge with missing or invalid outVertID");
        }

        int count = 0;
        for(Map<String, Object> currEdge : edges.values()) {
            if( currEdge.get("relation").equals(relation) && 
                currEdge.get("outVertID").equals(outVertID)  && 
                currEdge.get("inVertID").equals(inVertID) ) {
                count++;
            }
        }
        return count;
    }

    /**
     * get a list of vertex IDs based on a list of constraints
     * @param constraints list of constraints
     * @return list of vertex IDs
     */
    public List<String> getVertIDsByConstraints(List<DBConstraint> constraints){
        Set<String> candidateIDs = null;
        Set<String> nonMatchingIDs = new HashSet<String>();
        List<String> matchingIDs = new LinkedList<String>();

        //First, generate candidateIDs set.
        //Note that after candidateIDs is populated here, it will not be modified.
        if(indexedVertFields.size() > 0){ //TODO: indices
            //This should use indexed fields to find candidateIDs, then find the nonMatchingIDs below as usual.
            //we need to decide if only exact matches are allowed, or if ranges & etc. are ok here.
            //also, somehow indicate that the constraints used here are 'done', so they aren't re-checked below.
            candidateIDs = new HashSet<String>();
        }
        if(candidateIDs == null){ 
            //if no initial matchingIDs set was generated yet, use all IDs
            candidateIDs = vertices.keySet();
        }

        //make set of non-matching candidates, based on constraints
        for(String id : candidateIDs){
            Map<String, Object> candidateVert = vertices.get(id);
            for(DBConstraint c : constraints){
                Object candidateValue = candidateVert.get(c.getProp());
                if( !compare(candidateValue, c.getCond(), c.getVal()) ){
                    nonMatchingIDs.add(id);
                    break;
                }
            }
        }

        // build the matchingIDs list, based on candidateIDs and nonMatchingIDs
        for(String id : candidateIDs){
            if( !nonMatchingIDs.contains(id) ){
                matchingIDs.add(id);
            }
        }

        return matchingIDs;
    }

    /**
     * method to compare two objects that can use the conditional object
     * @param o1
     * @param cond
     * @param o2
     * @return true or false
     */
    private boolean compare(Object o1, Condition cond, Object o2){

        //TODO: confirm that this is the best way to handle these cases.
        if(o1 == null && cond == Condition.eq && o2 == null)
            return true;
        if ((o1 == null && o2 != null) && cond == Condition.notin )
            return true;
            
        if(o1 == null || o2 == null)
            return false;

        if(cond == Condition.eq){
            return o1.equals(o2);
        }
        if(cond == Condition.neq){
            return !o1.equals(o2);
        }
        if(cond == Condition.gt){
            if(o1 instanceof Comparable && o2 instanceof Comparable){
                Comparable c1 = (Comparable)o1;
                Comparable c2 = (Comparable)o2;
                return ( c1.compareTo(c2) > 0 );
            }else{
                return false;
            }
        }
        if(cond == Condition.gte){
            if(o1 instanceof Comparable && o2 instanceof Comparable){
                Comparable c1 = (Comparable)o1;
                Comparable c2 = (Comparable)o2;
                return ( c1.compareTo(c2) >= 0 );
            }else{
                return false;
            }
        }
        if(cond == Condition.lt){
            if(o1 instanceof Comparable && o2 instanceof Comparable){
                Comparable c1 = (Comparable)o1;
                Comparable c2 = (Comparable)o2;
                return ( c1.compareTo(c2) < 0 );
            }else{
                return false;
            }
        }
        if(cond == Condition.lte){
            if(o1 instanceof Comparable && o2 instanceof Comparable){
                Comparable c1 = (Comparable)o1;
                Comparable c2 = (Comparable)o2;
                return ( c1.compareTo(c2) <= 0 );
            }else{
                return false;
            }
        }
        if(cond == Condition.in){
            return contains(o1, o2);
        }
        if(cond == Condition.notin){
            return !contains(o1, o2);
        }

        return false;
    }

    /**
     * Determines based on object type whether a value (in the incoming lists) is contained
     * in the other other incoming list 
     * @param o1
     * @param o2
     * @return true or false
     */
    private boolean contains(Object o1, Object o2){
        //TODO: confirm that all of these are behaving as a user would expect for all type combinations.
        //eg. "asdf4.222" does not contain (Double)4.2 or (Integer)4
        //[101.0, 102.0] does not contain 101, and [101, 102] does not contain 101.0
        if(o1 instanceof Collection){
            Collection c1 = (Collection)o1;
            return c1.contains(o2);
        }else if(o1 instanceof byte[]){
            byte[] a1 = (byte[])o1;
            for(int i=0; i<a1.length; i++){
                //System.out.println("val is " + a1[i]);
                if( ((Byte)a1[i]).equals(o2)) return true;
            }
        }else if(o1 instanceof short[]){
            short[] a1 = (short[])o1;
            for(int i=0; i<a1.length; i++){
                //System.out.println("val is " + a1[i]);
                if( ((Short)a1[i]).equals(o2)) return true;
            }
        }else if(o1 instanceof int[]){
            int[] a1 = (int[])o1;
            for(int i=0; i<a1.length; i++){
                //System.out.println("val is " + a1[i]);
                if( ((Integer)a1[i]).equals(o2)) return true;
            }
        }else if(o1 instanceof long[]){
            long[] a1 = (long[])o1;
            for(int i=0; i<a1.length; i++){
                //System.out.println("val is " + a1[i]);
                if( ((Long)a1[i]).equals(o2)) return true;
            }
        }else if(o1 instanceof float[]){
            float[] a1 = (float[])o1;
            for(int i=0; i<a1.length; i++){
                //System.out.println("val is " + a1[i]);
                if( ((Float)a1[i]).equals(o2)) return true;
            }
        }else if(o1 instanceof double[]){
            double[] a1 = (double[])o1;
            for(int i=0; i<a1.length; i++){
                //System.out.println("val is " + a1[i]);
                if( ((Double)a1[i]).equals(o2)) return true;
            }
        }else if(o1 instanceof boolean[]){
            boolean[] a1 = (boolean[])o1;
            for(int i=0; i<a1.length; i++){
                //System.out.println("val is " + a1[i]);
                if( ((Boolean)a1[i]).equals(o2)) return true;
            }
        }else if(o1 instanceof char[]){
            char[] a1 = (char[])o1;
            for(int i=0; i<a1.length; i++){
                //System.out.println("val is " + a1[i]);
                if( ((Character)a1[i]).equals(o2)) return true;
            }
        }else if(o1 instanceof Object[]){
            //System.out.println("Array is " + (Object[])o1);
            return Arrays.asList((Object[])o1).contains(o2);
        }
        return false;
    }

    /**
     * remove a vertex by a vertex ID
     * @param vertID
     */
    public void removeVertByID(String vertID){
        Object nameObj = vertices.get(vertID).get("name");
        if(nameObj == null || !(nameObj instanceof String) ){
            throw new IllegalStateException("bad state: vertex must contain name field");
        }

        String name = (String)nameObj;
        vertIDs.remove(name);
        vertices.remove(vertID);
        
        //TODO: remove edges that contain this vertID!!!!!
    }

    /**
     * add a vertex given a property map
     * @param vert - property map
     * @return vertexID
     */
    public String addVertex(Map<String, Object> vert){
        Object nameObj = vert.get("name");
        if(nameObj == null || !(nameObj instanceof String) || ((String)nameObj).equals("") ){
            throw new IllegalArgumentException("cannot add vertes with empty name field");
        }//TODO check any other mandatory fields


        
        String name = (String)nameObj;
        if(vertIDs.containsKey(name)){
            removeVertByID(getVertIDByName(name));
        }
        String vertID = String.valueOf( UUID.randomUUID() );
        vertIDs.put(name, vertID);
        
        // make sure all multi-value properties are sets
        convertAllMultiValuesToSet(vert);
        vertices.put(vertID, vert);
        //TODO: update any indices
        return vertID;
    }

    /**
     * add and edge 
     * @param inVertID ID of the incoming vertex edge
     * @param outVertID - ID of the outgoing vertex edge
     * @param relation - type of edge relation
     */
    public void addEdge(String inVertID, String outVertID, String relation){
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot add edge with missing or invlid relation");
        }
        if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
            throw new IllegalArgumentException("cannot add edge with missing or invalid inVertID");
        }
        if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
            throw new IllegalArgumentException("cannot add edge with missing or invalid outVertID");
        }
        //TODO: check if edge is duplicate??  For now, just add it, duplicates are ok I guess.

        Map<String, Object> newEdge = new HashMap<String, Object>();
        newEdge.put("inVertID", inVertID);
        newEdge.put("outVertID", outVertID);
        newEdge.put("relation", relation);

        String edgeID = String.valueOf( UUID.randomUUID() );
        edges.put(edgeID, newEdge);
        //TODO: update any indices
    }

    /**
     * overwrite or add new properties to an existing vertex's property map
     * @param VertID
     * @param newVert - property map
     */
    public void updateVertex(String VertID, Map<String, Object> newVert){
        Map<String, Object> oldVert = vertices.get(VertID);
        if(oldVert == null){
            throw new IllegalArgumentException("invalid vertex ID");
        }
        Object newVertName = newVert.remove("name");
        Object oldVertName = oldVert.get("name");
        if(newVertName != null && !(((String)newVertName).equals((String)oldVertName)) ){
            throw new IllegalArgumentException("cannot update name of existing vertex");
        }

        for(Map.Entry<String, Object> entry: newVert.entrySet()){
            
            String key = entry.getKey();
            Object newValue = entry.getValue();
            updateVertexProperty(VertID, key, newValue);
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void open() {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeEdgeByRelation(String inVertID, String outVertID, String relation){
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot add edge with missing or invlid relation");
        }
        if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
            throw new IllegalArgumentException("cannot add edge with missing or invalid inVertID");
        }
        if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
            throw new IllegalArgumentException("cannot add edge with missing or invalid outVertID");
        }
        
        // collect the edge IDs that need to be removed
        Set<String> edgeIDs = new HashSet<String>();
        for(Map.Entry<String, Map<String, Object>> entry : edges.entrySet()) {
            String edgeID = entry.getKey();
            Map<String, Object> currEdge = entry.getValue();
            if( currEdge.get("relation").equals(relation) && 
                currEdge.get("outVertID").equals(outVertID)  && 
                currEdge.get("inVertID").equals(inVertID) ) {
                edgeIDs.add(edgeID);
            }
        }
        
        //remove the IDs we found
        for(String edgeID : edgeIDs) {
            edges.remove(edgeID);
        }
    }

    @Override
    public void removeAllVertices() {
        vertices.clear();
        vertIDs.clear();
        edges.clear();
        
    }

    @Override
    public DBConstraint getConstraint(String property, Condition condition,
            Object value) {
        
        return new InMemoryConstraint(property, condition, value);
    }

    @Override
    public void buildIndex(String indexConfig) {
        // NO-OP
        
    }
    
    @Override
    protected void setPropertyInDB(String id, String key, Object newValue) {
        
        vertices.get(id).put(key, newValue);
    }

    @Override
    public List<String> getInVertIDsByRelation(String v1, String relation, List<DBConstraint> constraints) {
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid relation");
        }
        if(v1 == null || v1.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid Vertex ID");
        }
        //TODO: consider faster implementations
        Set<String> constraintIDs = new HashSet<String>(getVertIDsByConstraints(constraints));
        Set<String> neighborIDs = new HashSet<String>(getInVertIDsByRelation(v1, relation));
        constraintIDs.retainAll(neighborIDs);
        List<String> ret = new LinkedList<String>(constraintIDs);
        return ret;
    }

    @Override
    public List<String> getOutVertIDsByRelation(String v1, String relation, List<DBConstraint> constraints) {
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid relation");
        }
        if(v1 == null || v1.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid Vertex ID");
        }
        //TODO: consider faster implementations
        Set<String> constraintIDs = new HashSet<String>(getVertIDsByConstraints(constraints));
        Set<String> neighborIDs = new HashSet<String>(getOutVertIDsByRelation(v1, relation));
        constraintIDs.retainAll(neighborIDs);
        List<String> ret = new LinkedList<String>(constraintIDs);
        return ret;
    }

    @Override
    public List<String> getVertIDsByRelation(String v1, String relation, List<DBConstraint> constraints) {
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid relation");
        }
        if(v1 == null || v1.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid Vertex ID");
        }
        //TODO: consider faster implementations
        Set<String> constraintIDs = new HashSet<String>(getVertIDsByConstraints(constraints));
        Set<String> neighborIDs = new HashSet<String>(getVertIDsByRelation(v1, relation));
        constraintIDs.retainAll(neighborIDs);
        List<String> ret = new LinkedList<String>(constraintIDs);
        return ret;
    }

}
