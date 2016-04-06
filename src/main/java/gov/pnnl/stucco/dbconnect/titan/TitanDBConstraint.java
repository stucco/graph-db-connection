package gov.pnnl.stucco.dbconnect.titan;

import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConstraintBase;

/**
 * This class is the concrete implementation of the conditional for
 * Titan comparison types.
 *
 */
public class TitanDBConstraint extends DBConstraintBase {

    /**
     * Constructor
     * @param property
     * @param condition
     * @param value
     */
    public TitanDBConstraint(String property, Condition condition, Object value){
        super(property, condition, value);
    }

    /**
     * Converts the conditional to the DB specific string type it needs
     */
    public String condString(Condition c){
        if(c == Condition.eq) return "T.eq";
        if(c == Condition.neq) return "T.neq";
        if(c == Condition.gt) return "T.gt";
        if(c == Condition.gte) return "T.gte";
        if(c == Condition.lt) return "T.lt";
        if(c == Condition.lte) return "T.lte";
        if(c == Condition.in) return "T.in";
        if(c == Condition.notin) return "T.notin";
        return null;
    }

}
