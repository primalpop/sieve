package model.policy;

/**
 * Created by cygnus on 9/25/17.
 */
public class ObjectCondition extends BooleanCondition  {
    public ObjectCondition(String attribute, RelOperator operator, String value) {
        super(attribute, operator, value);
    }
}
