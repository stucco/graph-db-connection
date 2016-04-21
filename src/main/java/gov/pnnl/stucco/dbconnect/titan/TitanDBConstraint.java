package gov.pnnl.stucco.dbconnect.titan;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

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
        if(c == Condition.contains) return "T.in";
        if(c == Condition.substring) return "Text.REGEX";
        return null;
    }
    
    /**
     *  converts specific conditional types to the form Titan needs them to be.
     * @param value
     * @return
     */
    public Object constructQueryObject(Object value)
    {
        Object newValue = value;
        Condition c = getCond();
        if(c == Condition.contains)
        {
            if (newValue instanceof Collection )
            {
                throw new IllegalArgumentException();
            } 
            // put it a set
            newValue = Collections.singleton(value);
        }
        else if(c == Condition.substring)
        {
            // the incoming value is a string
            // .*\\Q<value>\\E.*
            // the value must escape regex characters
            newValue = String.format(".*\\Q%s\\E.*", newValue.toString());
        }

        return newValue;
    }

}
