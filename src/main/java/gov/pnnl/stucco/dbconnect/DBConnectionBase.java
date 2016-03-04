package gov.pnnl.stucco.dbconnect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;

public abstract class DBConnectionBase implements DBConnectionAlignment, DBConnectionTestInterface, DBConnectionIndexerInterface {

    private Map<String,String> cardinalityCache = new HashMap<String, String>(200);

    public DBConnectionBase() {
        // TODO Auto-generated constructor stub
    }
    
    /**
     * returns cardinality of property "key" from vertex id.  If not found, returns null.
     */
    protected String findCardinality(String id, String key, Object val) {
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
        } else {
            
        }
        if(cardinality == null){
            cardinality = "SINGLE";
            cardinalityCache.put(key, cardinality);
        }

        return cardinality;
    }
    
    protected void updateVertexProperty(String id, String key, Object value)
    {
     // Determine new cardinality
        Object newValue = convertMultiValueToSet(value);
        String newCardinality = getCardinality(newValue);

        // Get old cardinality
        Object oldValue = null;
        // Try cache first
        String oldCardinality = cardinalityCache.get(key);
        if (oldCardinality == null){
            // Not in cache, so try DB
            Map<String, Object> queryRetMap = getVertByID(id);
            if (queryRetMap != null) {
                oldValue = queryRetMap.get(key);
                if(oldValue == null) {
                    // Not in DB, so use value and cardinality that will work
                    oldCardinality = newCardinality;
                    oldValue = Collections.emptySet();
                } else {
                    oldCardinality = getCardinality(oldValue);
                }
                
                // Cache cardinality 
                cardinalityCache.put(key, oldCardinality);
            } else {
                // id is bad, no data returned
            }
            
        }
        

        // Check for mismatch
        if (!oldCardinality.equals(newCardinality)) {
            String msg = String.format("Mismatch in cardinality for key: %s  and vertex ID: %s",key, id);
            throw new IllegalArgumentException(msg);
        }

        if (newCardinality.equals("SET"))
        {
            if(oldValue == null) {
                // For sets, we need the old value
                Map<String, Object> queryRetMap = getVertByID(id);
                if (queryRetMap != null) {
                    oldValue = queryRetMap.get(key);
                    if(oldValue == null) {
                        oldCardinality = newCardinality;
                        oldValue = Collections.emptySet();
                    } else {
                        oldCardinality = getCardinality(oldValue);
                    }
                } else {
                    // bad id
                }
            }
            
            // put the old and new together
            Set oldSet = (Set) oldValue;
            Set newSet = (Set) newValue;
            newSet.addAll(oldSet);
            

        }
        
//      set new value in DB  // <-- DB CALL
        setPropertyInDB(id, key, newValue);

    }
    

    /**
     * inserts this properties new value into the specified vertex ID and key
     * @param id
     * @param key
     * @param newValue
     */
    protected abstract void setPropertyInDB(String id, String key, Object newValue);
    
    /** Gets whether the value's data type supports a cardinality of "SET". */
    static protected boolean isMultipleCardinality(Object value) {
        return (value != null && (value instanceof JSONArray || value instanceof Set || value instanceof List || value instanceof Object[]));
    }
    
    /** gets the cardinality of the value */
    static protected String getCardinality(Object value) {
        return isMultipleCardinality(value)? "SET" : "SINGLE";
    }

    /**
     * Converts multi-valued Object to List, but leaves other Objects alone.
     */
    @SuppressWarnings("unchecked")
    static protected Object convertMultiValueToList(Object value) {
        
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
     * Converts multi-valued Object to List, but leaves other Objects alone.
     */
    @SuppressWarnings("unchecked")
    static protected Object convertMultiValueToSet(Object value) {
        
        Set newValue = new HashSet();
        if (value instanceof List) {
            newValue.addAll((List) value);
        }
        else if (value instanceof JSONArray ) {
            for(int i=0; i<((JSONArray)value).length(); i++){
                Object currVal = ((JSONArray)value).get(i);
                newValue.add(currVal);
            }
        }
        else if(value instanceof Object[]) {
            newValue.addAll(Arrays.asList((Object[])value));
        }
        else if(value instanceof int[]) {
            for (int i : (int[])value) {
                newValue.add(i);
            }
        } 
        else if(value instanceof Integer[]) {
            newValue.addAll(Arrays.asList((Integer[])value));
        
        }else {
            return value;
        }
        
        //TODO: add other primative types
        
        return newValue;
    }
    
    /**
     * take a map of properties and converts any that are multi-valued to sets
     * @param properties
     * @return properties
     */
    protected Map<String, Object> convertAllMultiValuesToSet(Map<String, Object> properties) 
    {
        for(Map.Entry<String, Object> entry : properties.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            Object newValue = convertMultiValueToSet(value);
            if(newValue != value) {
                properties.put(key, newValue);
            }
        }
        return properties;
    }
    
    /** Converts a Set to an Object array. */
    static public Object[] convertSetToArray(Set value) {
        List valueList = new ArrayList(value);
        return valueList.toArray();
    }
    
    /**
     * converts a JSONArray to an List
     * @param a
     * @return
     */
    static protected List<Object> jsonArrayToList(JSONArray a){
        List<Object> l = new ArrayList<Object>();
        for(int i=0; i<a.length(); i++){
            l.add(a.get(i));
        }
        return l;
    }
    
    public Map<String, Object> jsonVertToMap(JSONObject v){
        Map<String, Object> vert = new HashMap<String, Object>();
        for(Object k : v.keySet()){
            String key = (String) k;
            Object value = v.get(key);
            if(value instanceof JSONArray){
                value = DBConnectionBase.jsonArrayToList((JSONArray)value);
            }
            vert.put(key, value);
        }
        return vert;
    }
}
