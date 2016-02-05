package gov.pnnl.stucco.dbconnect.inmemory;


import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConnectionAlignment;
import gov.pnnl.stucco.dbconnect.DBConnectionTestInterface;
import gov.pnnl.stucco.dbconnect.DBConstraint;
import gov.pnnl.stucco.dbconnect.InvalidArgumentException;
import gov.pnnl.stucco.dbconnect.InvalidStateException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

/**
 * This class represents a concrete implementation of an in-memory DB Connection Type
 *
 */
public class InMemoryDBConnection implements DBConnectionAlignment, DBConnectionTestInterface {

    /** logger variable to track activities in this class*/
	private Logger logger = null;
	
	/** contains all the vertices via a map of maps where the values are a property map*/
	private Map<String, Map<String, Object>> vertices = null;
	
	/** contains a mapping of vertexIDs to the actual vertex canonical name*/
	private Map<String, String> vertIDs = null;
	
	/** contains a map of edges and their properties*/
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
	 * @throws InvalidStateException (e.g., can't find the vertex)
	 */
	public Map<String,Object> getVertByName(String vertName) throws InvalidStateException{
		if(vertName == null || vertName == "")
			return null;
		String id = vertIDs.get(vertName);
		if(id == null)
			return null;
		Map<String, Object> retVal = vertices.get(id);
		if(retVal == null)
			throw new InvalidStateException("bad state: known vertex name has no known content.");
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
	 * @throws InvalidArgumentException
	 */
	public List<String> getInVertIDsByRelation(String outVertID, String relation) throws InvalidArgumentException{
		if(relation == null || relation.equals("") ){
			throw new InvalidArgumentException("cannot get edge with missing or invlid relation");
		}
		if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
			throw new InvalidArgumentException("cannot get edge with missing or invalid outVertID");
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
     * @throws InvalidArgumentException
     */
	public List<String> getOutVertIDsByRelation(String inVertID, String relation) throws InvalidArgumentException{
		if(relation == null || relation.equals("") ){
			throw new InvalidArgumentException("cannot get edge with missing or invlid relation");
		}
		if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
			throw new InvalidArgumentException("cannot get edge with missing or invalid inVertID");
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
	 * @throws InvalidArgumentException
	 */
	public List<String> getVertIDsByRelation(String vertID, String relation) throws InvalidArgumentException{
		if(relation == null || relation.equals("") ){
			throw new InvalidArgumentException("cannot get edge with missing or invlid relation");
		}
		if(vertID == null || vertID.equals("") || !vertices.containsKey(vertID)){
			throw new InvalidArgumentException("cannot get edge with missing or invalid inVertID");
		}
		
		List<String> relatedIDs = new LinkedList<String>();
		for(Map<String, Object> currEdge : edges.values()){
			if( ((String)currEdge.get("relation")).equals(relation) ){
				if( ((String)currEdge.get("inVertID")).equals(vertID) || ((String)currEdge.get("outVertID")).equals(vertID)){
					relatedIDs.add( (String)currEdge.get("outVertID") ); //TODO: check valid state here?
				}
			}
		}
		return relatedIDs;
	}
	
	/**
	 * get the list of edge IDs based on incoming vertex and outgoing vertex and a relationship
	 * @param inVertID
	 * @param outVertID
	 * @param relation
	 * @return list of edge IDs
	 * @throws InvalidArguement Exception
	 */
	public List<String> getEdgeIDsByVert(String inVertID, String outVertID, String relation) throws InvalidArgumentException{
		if(relation == null || relation.equals("") ){
			throw new InvalidArgumentException("cannot get edge with missing or invlid relation");
		}
		if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
			throw new InvalidArgumentException("cannot get edge with missing or invalid inVertID");
		}
		if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
			throw new InvalidArgumentException("cannot get edge with missing or invalid outVertID");
		}
		
		List<String> edgeIDs = new LinkedList<String>();
		for( String k : edges.keySet() ){
			Map<String, Object> currEdge = edges.get(k);
			if( ((String)currEdge.get("relation")).equals(relation) ){
				if( ((String)currEdge.get("inVertID")).equals(inVertID) ){
					if( ((String)currEdge.get("outVertID")).equals(outVertID) ){
						edgeIDs.add(k);
					}
				}
			}
		}
		return edgeIDs;
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
				if( !compare(candidateVert.get(c.getProp()), c.getCond(), c.getVal()) ){
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
		}else if(o1 instanceof String){
			String s1 = (String)o1;
			//System.out.println("String is " + s1);
			if(o2 instanceof CharSequence || o2 instanceof Character)
				return s1.contains(o2.toString());
			else
				return false;
		}
		return false;
	}
	
	/**
	 * for a given edge ID return it's property map
	 * @param edgeID
	 * @return property map of the edge
	 */
	//TODO: shouldn't this throw an exception?
	public Map<String,Object> getEdgeByID(String edgeID){
		return edges.get(edgeID);
	}
	
	/**
	 * remove an edge for a given edge ID
	 * @param edgeID
	 * return property map of edge
	 */
	//TODO: shoudln't this throw an exception
	public Map<String,Object> removeEdgeByID(String edgeID){
		//TODO: update any indices
		return edges.remove(edgeID);
	}
	
	/**
	 * remove a vertex by a vertex ID
	 * @param vertID
	 * @return property map of the vertex
	 * @throws InvalidStateException (when the vertex ID isn't found)
	 */
	public Map<String,Object> removeVertByID(String vertID) throws InvalidStateException{
		Object nameObj = vertices.get(vertID).get("name");
		if(nameObj == null || !(nameObj instanceof String) ){
			throw new InvalidStateException("bad state: vertex must contain name field");
		}
		
		String name = (String)nameObj;
		vertIDs.remove(name);
		//TODO: update any indices
		return vertices.remove(vertID);
	}
	
	/**
     * add a vertex given a property map
     * @param vert - property map
     * @return vertexID
     * @throws InvalidStateException
     * @throws InvalidArgumentException
     */
	public String addVertex(Map<String, Object> vert) throws InvalidArgumentException, InvalidStateException{
		Object nameObj = vert.get("name");
		if(nameObj == null || !(nameObj instanceof String) || ((String)nameObj).equals("") ){
			throw new InvalidArgumentException("cannot add vertes with empty name field");
		}//TODO check any other mandatory fields
		
		String name = (String)nameObj;
		if(vertIDs.containsKey(name)){
			removeVertByID(getVertIDByName(name));
		}
		String vertID = String.valueOf( UUID.randomUUID() );
		vertIDs.put(name, vertID);
		vertices.put(vertID, vert);
		//TODO: update any indices
		return vertID;
	}
	
	/**
	 * add and edge 
	 * @param inVertID ID of the incoming vertex edge
	 * @param outVertID - ID of the outgoing vertex edge
	 * @param relation - type of edge relation
	 * @return edge ID
	 * @throws InvalidArgumentException
	 */
	public String addEdge(String inVertID, String outVertID, String relation) throws InvalidArgumentException{
		if(relation == null || relation.equals("") ){
			throw new InvalidArgumentException("cannot add edge with missing or invlid relation");
		}
		if(inVertID == null || inVertID.equals("") || !vertices.containsKey(inVertID)){
			throw new InvalidArgumentException("cannot add edge with missing or invalid inVertID");
		}
		if(outVertID == null || outVertID.equals("") || !vertices.containsKey(outVertID)){
			throw new InvalidArgumentException("cannot add edge with missing or invalid outVertID");
		}
		//TODO: check if edge is duplicate??  For now, just add it, duplicates are ok I guess.
		
		Map<String, Object> newEdge = new HashMap<String, Object>();
		newEdge.put("inVertID", inVertID);
		newEdge.put("outVertID", outVertID);
		newEdge.put("relation", relation);
		
		String edgeID = String.valueOf( UUID.randomUUID() );
		edges.put(edgeID, newEdge);
		//TODO: update any indices
		return edgeID;
	}
	
	/**
	 * overwrite or add new properties to an existing vertex's property map
	 * @param VertID
	 * @param newVert - property map
	 * @throws InvalidArgumentException
	 */
	public void updateVertex(String VertID, Map<String, Object> newVert) throws InvalidArgumentException{
		Map<String, Object> oldVert = vertices.get(VertID);
		if(oldVert == null){
			throw new InvalidArgumentException("invalid vertex ID");
		}
		Object newVertName = newVert.remove("name");
		Object oldVertName = oldVert.get("name");
		if(newVertName != null && !(((String)newVertName).equals((String)oldVertName)) ){
			throw new InvalidArgumentException("cannot update name of existing vertex");
		}
		
		for(String k: newVert.keySet()){
			oldVert.put(k, newVert.get(k));
		}
		//TODO: update any indices
	}
	
	/**
	 * commit transaction
	 */
	private void commit(){
	}
	
	//tries to commit, returns true if success.
	private boolean tryCommit(){
		try{
			commit();
		}catch(Exception e){
			return false;
		}
		return true;
	}
	
	//TODO:  Why do we have tryCommit and waitFor for the in-memory DB?
	
	//tries to commit, up to 'limit' times. returns true if success.
	private boolean tryCommit(int limit){
		int count = 0;
		boolean result = false;
		while(!result && count < limit){
			result = tryCommit();
			count += 1;
		}
		return result;
	}

	/**
	 * sleep for a bit as part of a commit, in case the DB is 
	 * @param ms
	 */
	private void waitFor(int ms){
		try {
			Thread.sleep(ms);
		}
		catch (InterruptedException ie) { 
			// Restore the interrupted status
			Thread.currentThread().interrupt();
		}
	}
	
	/**
	 * Way to debug when exceptions can happen
	 * @param e
	 * @return
	 */
	private static String getStackTrace(Exception e){
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}
	
	//see Align class
	/**
	 * method to convert a JSON array to a Java List
	 * @param a
	 * @return
	 */
	public List<Object> jsonArrayToList(JSONArray a){
		List<Object> l = new ArrayList<Object>();
		for(int i=0; i<a.length(); i++){
			l.add(a.get(i));
		}
		return l;
	}
	
	//see Align class	
	/**
	 * converts a JSON Vertex to a Java Map
	 * @param v
	 * @return property map for the Vertex
	 */
	@Override
    public Map<String, Object> jsonVertToMap(JSONObject v){
		Map<String, Object> vert = new HashMap<String, Object>();
		for(Object k : v.keySet()){
			String key = (String) k;
			Object value = v.get(key);
			if(value instanceof JSONArray){
				value = jsonArrayToList((JSONArray)value);
			}
			else if(value instanceof JSONObject){
				logger.warn("jsonVertToMap: unexpected property type: JSONObject for property " + key + "\n" + v);
			}
			vert.put(key, value);
		}
		return vert;
	}


	/**
	 * get an Edge by its ID
	 * @param id 
	 * @return property map of edge
	 */
    @Override
    public Map<String, Object> getEdgeById(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void open() {
        // TODO Auto-generated method stub
        
    }

}
