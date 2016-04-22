package gov.pnnl.stucco.dbconnect;

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
    contains,     // contained in a collection
    substring   // contained in a string
}