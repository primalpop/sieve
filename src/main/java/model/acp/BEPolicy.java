package model.acp;

import model.acp.ToInclude.PolicyMetadata;

import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class BEPolicy {

    private String id;

    private String description;

    /**
     * 1) user = Alice ^ loc = 2065
     * 2) user = Bob ^ time > 5pm
     * 3) user = Alice ^ 5 pm < time < 7 pm
     */
    private List<ObjectCondition> object_conditions;

    /**
     * 1) querier = John
     * 2) querier = John ^ time = 6 pm
     * 3) querier = John ^ location = 2065
     */

    private List<QuerierCondition> querier_conditions;

    /**
     * 1) Analysis
     * 2) Concierge
     * 3) Noodle
     */
    private String purpose;

    /**
     * 1) Allow
     * 2) Deny
     */
    private String action;

    public void print(){
        System.out.println
    }

    public BEPolicy(String id, String description, List<ObjectCondition> object_conditions, List<QuerierCondition> querier_conditions, String purpose, String action) {
        this.id = id;
        this.description = description;
        this.object_conditions = object_conditions;
        this.querier_conditions = querier_conditions;
        this.purpose = purpose;
        this.action = action;
    }
}
