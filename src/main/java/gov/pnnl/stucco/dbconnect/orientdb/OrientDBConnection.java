package gov.pnnl.stucco.dbconnect.orientdb;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConnectionAlignment;
import gov.pnnl.stucco.dbconnect.DBConnectionIndexerInterface;
import gov.pnnl.stucco.dbconnect.DBConnectionTestInterface;
import gov.pnnl.stucco.dbconnect.DBConstraint;
import gov.pnnl.stucco.dbconnect.StuccoDBException;

public class OrientDBConnection implements DBConnectionAlignment, DBConnectionTestInterface, DBConnectionIndexerInterface {
   
    private OrientGraph graphDB = null;
    private Logger logger = null;
    private Map<String, String> configuration = new HashMap<String,String>();
    private Map<String, String> vertIDCache; //TODO could really split this into a simple cache class.
    private Set<String> vertIDCacheRecentlyRead;
    private static int VERT_ID_CACHE_LIMIT = 10000;
    private Map<String,String> cardinalityCache = new HashMap<String, String>(200);
    private static String[] HIGH_FORWARD_DEGREE_EDGE_LABELS = {"hasFlow"}; //TODO: update as needed.  Knowing these allows some queries to be optimized.

    
    public OrientDBConnection(Map<String, String> configuration) {
        this.configuration.putAll(configuration);
        logger = LoggerFactory.getLogger(OrientDBConnection.class);
        vertIDCache = new HashMap<String, String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
        vertIDCacheRecentlyRead = new HashSet<String>((int) (VERT_ID_CACHE_LIMIT * 1.5));

    }

    @Override
    public void updateVertex(String id, Map<String, Object> properties)
            throws IllegalArgumentException, IllegalStateException 
    {
        String[] keys = properties.keySet().toArray(new String[0]);
        for(int i=0; i<keys.length; i++){
            updateVertProperty(id, keys[i], properties.get(keys[i]));
            graphDB.commit();
        }

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void updateVertProperty(String id, String key, Object val) throws OCommandExecutionException{ 
        String cardinality = findCardinality(id, key, val);
        if(cardinality == null){
            cardinality = "SINGLE";
            cardinalityCache.put(key, cardinality);
        }
        
        if (key.equals("_properties")) {
            return;
        }

        if (cardinality.equals("SET")) {
            // At this point, we assume it's in List form
            
            Map<String, Object> queryRetValue = getVertByID(id);
            if (queryRetValue == null) {
                // Handle how?
            }
            
            Object obj = convertMultiValueToList(val);
            List newValue;
            if (obj instanceof List) {
                newValue = (List) obj;
            }
            else {
                newValue = Collections.singletonList(obj);
            }
            
            List currentList = (List)queryRetValue.get(key);

            if(currentList == null) {
                // no value existed in the DB
                val = newValue;
            } else {
                Set currentSet = new HashSet(currentList);
                for (Object currVal : newValue) {
                    if (!currentSet.contains(currVal)) {
                        currentSet.add(currVal);
                        currentList.add(currVal);
                    }
                }
                val = currentList;
            }
        }

        String query = String.format("SELECT FROM %s",id);
        List<OrientVertex> verts = this.getVerticesFromQuery(query);
        if(!verts.isEmpty()) {
            verts.get(0).setProperty(key, val);
        }
    }
    
    /**
     * returns cardinality of property "key" from vertex id.  If not found, returns null.
     */
    private String findCardinality(String id, String key, Object val) throws OCommandExecutionException {
        String cardinality;

        cardinality = cardinalityCache.get(key);
        if(cardinality == null){
            if(isMultipleCardinality(val)){
                cardinality = "SET";
                cardinalityCache.put(key, cardinality);
            } else {
                // go to DB to see if it has this property, from this vertex id
                Map<String, Object> queryRetMap = getVertByID(id);  
                if (queryRetMap != null) {
                    Object dbVal = queryRetMap.get(key);
                    if (dbVal != null) {
                        if(isMultipleCardinality(dbVal)){
                            cardinality = "SET";
                        }
                        else {
                            cardinality = "SINGLE";
                        }
                        cardinalityCache.put(key, cardinality);
                    }
                } 
            }
        }
        return cardinality;
    }

    /** Gets whether the value's data type supports a cardinality of "SET". */
    private boolean isMultipleCardinality(Object value) {
        return (value != null && (value instanceof JSONArray || value instanceof Set || value instanceof List || value instanceof Object[]));
    }
    
    /**
     * Converts multi-valued Object to List, but leaves other Objects alone.
     */
    private Object convertMultiValueToList(Object value) {
        
        List newValue = new ArrayList();
        if (value instanceof Set) {
            newValue = new ArrayList((Set) value);
        }
        else if (value instanceof JSONArray ) {
            for(int i=0; i<((JSONArray)value).length(); i++){
                Object currVal = ((JSONArray)value).get(i);
                newValue.add(currVal);
            }
        }
        else if(value instanceof Object[]) {
            for(int i=0; i<((Object[])value).length; i++){ 
                Object currVal = ((Object[])value)[i];
                newValue.add(currVal);
            }
        } else {
            return value;
        }
        
        return newValue;
    }

    /** 
     * Runs SQL query to get vertices.
     * 
     * @throws OCommandExecutionException on bad query
     * 
     * @return Zero or more vertices
     * */
    private List<OrientVertex> getVerticesFromQuery(String query) throws OCommandExecutionException {
        OrientDynaElementIterable qiterable = executeSQL(query);
        List<OrientVertex> vertexList = new ArrayList<OrientVertex>(1);
        if (qiterable != null) { // Don't know if this can happen, but just in case
            Iterator<Object> iter = qiterable.iterator();
            while (iter.hasNext()) {
                vertexList.add((OrientVertex) iter.next());
            }
        }
        return vertexList;
    }

    /**
     *  Runs SQL query to do things like create properties, indexes, etc...
     * @param query
     * @return
     */
    public <T> T executeSQL(String query) {
        OCommandSQL sql = new OCommandSQL(query);
        OCommandRequest ocr = graphDB.command(sql);
        T obj = ocr.<T>execute();
        
        return obj;
    }
    
    @Override
    public Map<String, Object> jsonVertToMap(JSONObject v) {
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

    //see Align class
    private List<Object> jsonArrayToList(JSONArray a){
        List<Object> l = new ArrayList<Object>();
        for(int i=0; i<a.length(); i++){
            l.add(a.get(i));
        }
        return l;
    }
    
    /** 
     * {@inheritDoc}
     * <p>Gets the properties of a vertex selected by RID. 
     * 
     * TODO: deal with exceptions
     * @throws OCommandExecutionException on bad query
     * 
     * @return1 Map of properties (or null if vertex not found)
     */
    @Override
    public Map<String, Object> getVertByID(String id) {
        if(id == null || id.isEmpty())
            return null;
        
        String query = String.format("Select from %s", id);
        List<OrientVertex> vertexList = getVerticesFromQuery(query);
        if(vertexList.isEmpty()){
            return null;
        }
        
        return addPropertiesKey(vertexList).get(0);
    }

    /**
     *TODO: (SJB) determine if we need to do this anymore?
     * Adds the "_properties" key to the map that is copy of the original map.
     * 
     * @param vertexList
     * 
     * @return  List of a Map of properties (or an empty list if no vertices)
     */
    private List<Map<String, Object>> addPropertiesKey(List<OrientVertex> vertexList)
    {
        List<Map<String,Object>> listPropertyMap = new ArrayList<Map<String,Object>>();
        Map<String, Object> propertyMap = null;
        for (OrientVertex v : vertexList) {
            propertyMap = v.getProperties();
            Map<String,Object> existingProperties = new HashMap<String, Object>(propertyMap);
            propertyMap.put("_properties", existingProperties);
            listPropertyMap.add(propertyMap);
        }
        return listPropertyMap;
    }
    
    @Override
    public Map<String, Object> getVertByName(String name){
        if(name == null || name.isEmpty())
            return null;

        String query = String.format("Select * from V where name='%s'", name);
        List<OrientVertex> vertexList = getVerticesFromQuery(query);

        if(vertexList.size() == 0){
            return null;
        }else if(vertexList.size() > 1){
            logger.warn("findVert found more than 1 matching verts for name: " + name + " so returning the first item.");
        }

        return addPropertiesKey(vertexList).get(0);
    }

    /**
     * {@inheritDoc}
     * <p>  function will check vertIDCache first, 
     * if id is not in there, then it is calling the findVert function
     */
    @Override
    public String getVertIDByName(String name) {
        String id = vertIDCacheGet(name);
        if(id != null){
            return id;
        }else{
            Map<String, Object> vert = getVertByName(name);
            if(vert == null) 
                id = null;
            else 
                id = vert.get("@rid").toString();
            if(id != null){
                vertIDCachePut(name, id);
            }
            return id;
        }
    }
    
    private void vertIDCachePut(String name, String id){
        if(vertIDCache.size() >= VERT_ID_CACHE_LIMIT){
            logger.info("vertex id cache exceeded limit of " + VERT_ID_CACHE_LIMIT + 
                    " ... evicting " + (vertIDCache.size() - vertIDCacheRecentlyRead.size()) + " unused items.");
            vertIDCache.keySet().retainAll(vertIDCacheRecentlyRead);
            vertIDCacheRecentlyRead.clear();
        }
        vertIDCache.put(name, id);
    }
    
    private String vertIDCacheGet(String name){
        if(vertIDCache.containsKey(name)){ 
            vertIDCacheRecentlyRead.add(name);
            return vertIDCache.get(name);
        }else{
            return null;
        }
        
    }

    @Override
    public String addVertex(Map<String, Object> properties) {
        String name = (String)properties.get("name");
        String id = (String)properties.get("_id");
        if(name == null || name.isEmpty()){
            name = id;
            properties.put("name", name);
        }
        
        //convert any multi-valued properties to a list form.
        for(Map.Entry<String, Object> entry : properties.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            Object newValue = convertMultiValueToList(value);
            if(newValue != value) {
                properties.put(key, newValue);
            }
        }
        properties.remove("_properties");

        properties.remove("_id"); //Some graphDB servers will ignore this ID, some won't.  Just remove them so it's consistent.
        OrientVertex v = graphDB.addVertex("class:V", properties);
        graphDB.commit();
        return v.getId().toString();
    }

    @Override
    public List<String> getVertIDsByConstraints(List<DBConstraint> constraints) {
        if(constraints == null || constraints.size() == 0)
            return null;

        Map<String, Object> param = new HashMap<String, Object>();
        String query = String.format("SELECT FROM V where ");
        for(int i=0; i<constraints.size(); i++){
            DBConstraint c = constraints.get(i);
            String cond = c.condString(c.getCond());
            String key = c.getProp().toUpperCase()+i;
            param.put(key, c.getVal());
            if(i > 0 ) {
                query += " AND ";
            }
            query += String.format(" %s %s '%s' ", c.getProp(), cond, c.getVal());
        }
        
        List<OrientVertex> verts = this.getVerticesFromQuery(query);
        List<String> vertIDs = new ArrayList<String>();
        for (OrientVertex vertID : verts ){
            vertIDs.add(vertID.getId().toString());
        }
        
        return vertIDs;
    }

    @Override
    public void addEdge(String inVertID, String outVertID, String relation){
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot add edge with missing or invalid relation");
        }
        if(inVertID == null || inVertID.equals("") ){
            throw new IllegalArgumentException("cannot add edge with missing or invalid inVertID");
        }
        if(outVertID == null || outVertID.equals("")){
            throw new IllegalArgumentException("cannot add edge with missing or invalid outVertID");
        }
       
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("inVertID", inVertID);
        props.put("outVertID", outVertID);
        props.put("relation", relation);
        
        //and now finally add edge to graphDB.  If it fails, return false here. if it was ok, then we can continue below.
        String query = String.format("Select * from [%s,%s]", outVertID, inVertID);
        List<OrientVertex> vertexList = getVerticesFromQuery(query);
        if(vertexList.size() == 2) {
            vertexList.get(0).addEdge(relation, vertexList.get(1), null /*iClassName*/, null /*iClusterName*/, props);
            
            // alternate way to create an edge using both vertex's, we need another call to specify the properties
//            String id = String.format("class:%s",relation);
//            OrientEdge e =graphDB.addEdge((Object)id, vertexList.get(0), vertexList.get(1), relation);
//            e.setProperties(props);
            graphDB.commit();
        } else {
            String msg = String.format("cannot add edge with missing or invalid vertID, either %s or %s", outVertID, inVertID);
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public List<String> getInVertIDsByRelation(String outVertID, String relation) {
        
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invlid relation");
        }
        if(outVertID == null || outVertID.equals("")){
            throw new IllegalArgumentException("cannot get edge with missing or invalid outVertID");
        }
        
        Object query_ret = getVertByID(outVertID);
        if(query_ret == null){
            logger.warn("getInVertIDsByRelation could not find outVertID:" + outVertID);
            throw new IllegalArgumentException("missing or invalid outVertID");
        }

        
        String query = String.format("SELECT expand(out('%s')) FROM %s",relation, outVertID); 
        List<OrientVertex>results = getVerticesFromQuery(query);

        List<String> relatedIDs = new ArrayList<String>();
        for(OrientVertex item : results){
            String idValue = item.getId().toString();
            relatedIDs.add(idValue);
//            String idvalue = item.getProperties().get("@rid").toString();
            
        }
        return relatedIDs;
    }

    @Override
    public List<String> getOutVertIDsByRelation(String inVertID, String relation){
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invlid relation");
        }
        if(inVertID == null || inVertID.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invalid outVertID");
        }
        
        Object query_ret = getVertByID(inVertID);
        if(query_ret == null){
            logger.warn("getOutVertIDsByRelation could not find inVertID:" + inVertID);
            throw new IllegalArgumentException("missing or invalid inVertID");
        }

        
        String query = String.format("SELECT expand(in('%s')) FROM %s",relation, inVertID); 
        List<OrientVertex>results = getVerticesFromQuery(query);

        List<String> relatedIDs = new ArrayList<String>();
        for(OrientVertex item : results){
            String idValue = item.getId().toString();
            relatedIDs.add(idValue);
//            String idvalue = item.getProperties().get("@rid").toString();
            
        }
        return relatedIDs;
    }

    @Override
    public List<String> getVertIDsByRelation(String vertID, String relation) {
        if(relation == null || relation.equals("") ){
            throw new IllegalArgumentException("cannot get edge with missing or invlid relation");
        }
        if(vertID == null || vertID.equals("")) {
            throw new IllegalArgumentException("cannot get edge with missing or invalid vertID");
        }
        
        Object query_ret = getVertByID(vertID);
        if(query_ret == null){
            logger.warn("getVertIDsByRelation could not find vertID:" + vertID);
            throw new IllegalArgumentException("missing or invalid vertID");
        }

        
        String query = String.format("SELECT expand(both('%s')) FROM %s",relation, vertID); 
        List<OrientVertex>results = getVerticesFromQuery(query);

        List<String> relatedIDs = new ArrayList<String>();
        for(OrientVertex item : results){
            String idValue = item.getId().toString();
            relatedIDs.add(idValue);
//            String idvalue = item.getProperties().get("@rid").toString();
            
        }
        return relatedIDs;
    }

    /*
     * Only used by removeAllVertices()
     */
    private void removeCachedVertices(){
        //NB: this query is slow enough that connection can time out if the DB starts with many vertices.

        if(vertIDCache.isEmpty())
            return;

        //clear the cache now.
        vertIDCache.clear();
        vertIDCacheRecentlyRead.clear();// = new HashSet<String>((int) (VERT_ID_CACHE_LIMIT * 1.5));
    }
    
    @Override
    public void removeVertByID(String id) {
        
        // remove the cached vertices
        removeCachedVertices();
        
        String query = String.format("DELETE VERTEX %s", id);
        executeSQL(query);
    }

    @Override
    public void removeEdgeByRelation(String v1, String v2, String relation) {
       
        String query = String.format("DELETE EDGE FROM %s TO %s WHERE relation=\"%s\"", v1, v2, relation);
        executeSQL(query);
        
    }

    @Override
    public int getEdgeCountByRelation(String inVertID, String outVertID, String relation) {
        int edgeCount = 0;
        
        Object query_ret;

        query_ret = getVertByID(outVertID);
        if(query_ret == null){
            logger.warn("getEdgeCount could not find out_id:" + outVertID);
            throw new IllegalArgumentException("invalid outVertID: "+ outVertID);
        }
        query_ret = getVertByID(inVertID);
        if(query_ret == null){
            logger.warn("getEdgeCount could not find inv_id:" + inVertID);
            throw new IllegalArgumentException("invalid inVertID: "+ inVertID);
        }

        String query = String.format("SELECT expand(out('%s')) FROM %s",relation, outVertID); 
        List<OrientVertex>results = getVerticesFromQuery(query);

        for(OrientVertex item : results){
            String idvalue = item.getProperties().get("@rid").toString();
            if(inVertID.equals(idvalue))
                edgeCount++;
        }
        return edgeCount;
    }
    
    @Override
    public void close() {
        if(graphDB != null){
            graphDB.shutdown();
        }
    }
    
    /**
     * need to commit this transaction
     */
    public void commit() {
        graphDB.commit();
    }

    @Override
    public void open() {
        logger.info("connecting to DB...");

        try {
            // extract configuration for the DB of interest
            String dbName = configuration.get("graph-name");
            graphDB = new OrientGraph(dbName);
        } catch (Exception e) {
            logger.warn(e.getLocalizedMessage());
            logger.warn(getStackTrace(e));
            graphDB.shutdown();
            throw new StuccoDBException("could not create OrientDB client connection");
        }
    }

    private static String getStackTrace(Exception e){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
    
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeAllVertices() {
        
        // remove the cached vertices
        removeCachedVertices();

        //NB: this query is slow enough that connection can time out if the DB starts with many vertices.
        String query = String.format("DELETE VERTEX V");
        executeSQL(query);
        
    }

    @Override
    public DBConstraint getConstraint(String property, Condition condition,
            Object value) {
        return new OrientDBConstraint(property, condition, value);
    }

    @Override
    public void buildIndex(String indexConfig) throws IOException {
        IndexDefinitionsToOrientDB loader = new IndexDefinitionsToOrientDB();
        loader.parse(new File(indexConfig));
        
    }
    
    /**
     * return the raw handle to the DB
     */
    public OrientGraph getGraphDB() {
        return graphDB;
    }


}
