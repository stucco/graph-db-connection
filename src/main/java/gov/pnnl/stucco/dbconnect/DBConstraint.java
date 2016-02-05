package gov.pnnl.stucco.dbconnect;

/**
 * This interface is the way we specify names, values and conditionals when searching for
 * or filtering out types of vertices.  The interface is required to deal with the various
 * implementation of the various DBs and how they implement that functionality.
 *
 */
public interface DBConstraint {

    /**
     * returns the string to be used as part of the conditional portion for a query to the DB
     * @param c - type of comparison
     * @return the DB syntax to be used for that DB
     */
    public String condString(Condition c);
    
    /**
     * Get the Condition associated with the DB type we are using to know how the comparison should be done
     * @return
     */
    public Condition getCond();
    
    /**
     * Get the property name that is associated with constraint
     * @return
     */
    public String getProp();
    
    /**
     * Get the property value that is to be used in the constraint
     * @return
     */
    public Object getVal();

}
