package gov.pnnl.stucco.dbconnect.orientdb;

import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConstraintBase;

/**
 * This class is the concrete implementation of the conditional for
 * OrientDB comparison types.
 *
 */
public class OrientDBConstraint extends DBConstraintBase {

    /**
     * Constructor
     * @param property
     * @param condition
     * @param value
     */
    public OrientDBConstraint(String property, Condition condition, Object value){
        super(property, condition, value);
    }

    /**
     * Converts the conditional to the DB specific string type it needs
     */
    public String condString(Condition c){
        if(c == Condition.eq) return "=";
        if(c == Condition.neq) return "<>";
        if(c == Condition.gt) return ">";
        if(c == Condition.gte) return ">=";
        if(c == Condition.lt) return "<";
        if(c == Condition.lte) return "<=";
        if(c == Condition.contains) return "CONTAINS";
        if(c == Condition.substring) return "CONTAINSTEXT";
        return null;
    }

}
