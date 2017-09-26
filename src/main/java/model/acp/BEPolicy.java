package model.acp;

import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class BEPolicy {

    private String id;

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
    private List<String> purpose;

    /**
     * 1) Allow
     * 2) Deny
     */
    private String action;


}
