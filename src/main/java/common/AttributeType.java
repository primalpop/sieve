package common;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Author primpap
 */
public enum AttributeType {

    @JsonProperty("STRING")
    STRING(1),
    @JsonProperty("TIMESTAMP")
    TIMESTAMP(2),
    @JsonProperty("DOUBLE")
    DOUBLE(3);

    private final int id;

    private static final Map<Integer, AttributeType> lookup = new HashMap<>();

    static {
        for (AttributeType d : AttributeType.values()) {
            lookup.put(d.getID(), d);
        }
    }

    private AttributeType(int id) {
        this.id = id;
    }

    public int getID() {
        return id;
    }

    public static AttributeType get(String id) {
        return lookup.get(id);

    }
}
