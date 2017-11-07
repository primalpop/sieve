package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by cygnus on 9/25/17.
 */
public enum RelOperator {
    @JsonProperty("=")
    EQUALS("="),
    @JsonProperty(">=")
    GEQ(">="),
    @JsonProperty("<=")
    LEQ("<=");

    private final String name;

    private static final Map<String, RelOperator> lookup = new HashMap<>();

    static {
        for (RelOperator d : RelOperator.values()) {
            lookup.put(d.getName(), d);
        }
    }

    private RelOperator(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static RelOperator get(String name) {
        return lookup.get(name);

    }
}
