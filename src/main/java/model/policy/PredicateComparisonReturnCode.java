package model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by cygnus on 10/26/17.
 */
public enum PredicateComparisonReturnCode {

    ATTR_EQ(0), //Attributes equal
    ATTR_NEQ(-1), //Attributes not equal
    PRED_NEQ(2), //Predicates not equal
    PRED_EQ(3); //Predicates equal

    private final int code;
    private PredicateComparisonReturnCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
