package model.acp;

/**
 * Created by cygnus on 9/25/17.
 */
public class QuerierCondition extends BooleanCondition {
    public QuerierCondition(String attribute, RelOperator operator, String value) {
        super(attribute, operator, value);
    }
}
