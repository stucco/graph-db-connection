package gov.pnnl.stucco.dbconnect;


/**
 * Base class for representing Constraints for filtering
 *
 */
public abstract class DBConstraintBase implements DBConstraint {
 
    /** conditional */
    private Condition cond;
    
    /** name of the property */
    private String prop;
    
    /** value of being used for the conditional*/
    private Object val;

    /**
     * Constructor
     * @param property
     * @param condition
     * @param value
     */
    public DBConstraintBase(String property, Condition condition, Object value){
        this.prop = property;
        this.cond = condition;
        this.val = value;
    }
    
    /**
     * get the Conditional
     */
    public Condition getCond() {
        return cond;
    }
    
    /**
     * set the value on the conditional
     * @param cond
     */
    public void setCond(Condition cond) {
        this.cond = cond;
    }
    
    /**
     * get the Property name
     */
    public String getProp() {
        return prop;
    }
    
    /**
     * set the Property name
     * @param prop
     */
    public void setProp(String prop) {
        this.prop = prop;
    }
    
    /**
     * get the value associate with this property to be used in the conditional
     */
    public Object getVal() {
        return val;
    }
    
    /**
     * set the value of the property which will be used in the conditional
     * @param val
     */
    public void setVal(Object val) {
        this.val = val;
    }

    
}
