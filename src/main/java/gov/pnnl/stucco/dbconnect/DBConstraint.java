package gov.pnnl.stucco.dbconnect;

// GN: I assume this class is adapted from Constraint.java.
//
// It looks like an interface is needed because the condString implementation
// needs to be different for each database. I'm guessing the enum implementation
// will be the same for all databases. In that case, I would just make it its
// own class, instead of being in the interface. If your preference is to keep
// Condition still somewhat under the control of the Constraint "namespace",
// then an alternative to putting it in the interface would be to put it in an
// abstract base class used as the foundation for implementations of the
// interface. Many of the core Java classes use this sort of pattern.

public interface DBConstraint {

    /**
     * the set of conditions that can be used for comparisons
     *
     */
    public enum Condition {
        gt,     // greater than
        gte,    // greater than or equal
        eq,     // equal to
        neq,    // not equal to
        lte,    // less than or equal to
        lt,     // less than
        in,     // inside (set operation)
        notin   // not inside (set operation)
    }

    /**
     * returns the string to be used as part of the conditional portion for a query to the DB
     * @param c - type of comparison
     * @return the DB syntax to be used for that DB
     */
    public String condString(Condition c);

}
