package gov.pnnl.stucco.dbconnect.elasticsearch;

import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConstraintBase;

public class ElasticsearchDBConstraint extends DBConstraintBase {

	public ElasticsearchDBConstraint(String property, Condition condition, Object value) {
		super(property, condition, value);
	}

	@Override
	public String condString(Condition c) {
		if (c == Condition.eq) return "+";
        if (c == Condition.neq) return "-";
//        if (c == Condition.gt) return ">";
//        if (c == Condition.gte) return ">=";
//        if (c == Condition.lt) return "<";
//        if (c == Condition.lte) return "<=";
//        if (c == Condition.contains) return ".must(QueryBuilders.fuzzyQuery(\"" + this.getProp() + "\", " + this.getVal().toString() + "))";
//        if (c == Condition.substring) return ".must(QueryBuilders.fuzzyQuery(\"" + this.getProp() + "\", " + this.getVal().toString() + "))";
        
		return null;
	}

}
