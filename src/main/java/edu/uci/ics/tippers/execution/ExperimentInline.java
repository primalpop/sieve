package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.util.List;

public class ExperimentInline {

    int querier;

    public ExperimentInline(){
        this.querier = 177;
    }

    /**
     * Experiment to determine the policy evaluation cost
     * TODO: Isolate read_cost and eval_cost, the current constant for policy_eval includes readt_cost as well
     * @param args
     */
    public static void main(String[] args){
        PolicyPersistor polper = new PolicyPersistor();
        MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();
        String query_predicates = ") Select * from polEval";
        String querier = "177";
        String guard_id = "'d62bdc8c-a081-4fe4-a5ca-38208d88847e'";
        String guard = "WITH polEval as (Select * from PRESENCE USE INDEX() WHERE (start_date<= \"2018-02-02\") " +
                "AND (start_date>= \"2018-02-01\")";
        String udf_query = " hybcheck(" + querier + ", "
                 + guard_id +  " , user_id, location_id, start_date, " +
                        "start_time, user_profile, user_group ) = 1";
        MySQLResult udfTime = mySQLQueryManager.runTimedQueryExp(guard + PolicyConstants.CONJUNCTION
                + udf_query + query_predicates, 1);
        System.out.println("UDF time " + udfTime.getTimeTaken().toMillis());
        String add_policy_1 = "((user_profile= \"facultys\") " +
                "AND (start_date<= \"2018-04-30\") AND (start_date>= \"2018-02-01\") " +
                "AND (start_time>= \"00:01:00\") AND (start_time<= \"23:59:00\"))";
        String add_policy_2 = "((user_profile= \"graduates\") " +
                "AND (start_date<= \"2018-03-30\") AND (start_date>= \"2018-02-01\") " +
                "AND (start_time>= \"10:12:00\") AND (start_time<= \"23:59:00\"))";

        for (int i = 50; i < 200; i=i+50) {
            String query = guard + " AND ((user_profile= \"graduate\") " +
                    "AND (start_date<= \"2018-06-30\") AND (start_date>= \"2018-05-30\") " +
                    "AND (start_time>= \"08:00:00\") AND (start_time<= \"17:00:00\")) " +
                    "OR ((user_profile= \"undergrad\") AND (start_date<= \"2018-01-30\") " +
                    "AND (start_date>= \"2018-02-01\") AND (start_time>= \"12:00:00\") AND (start_time<= \"17:00:00\"))  ";
            for (int j = 0; j < i; j++)
                if(j%2 == 0)
                    query += PolicyConstants.DISJUNCTION + add_policy_1;
                else
                    query += PolicyConstants.DISJUNCTION + add_policy_2;
            query += query_predicates;
            long total = 0;
            boolean first = false;
            for (int k = 0; k < 3; k++) {
                MySQLResult execTime = mySQLQueryManager.runTimedQueryExp(query, 1);
                if(!first) {
                    first = true;
                    continue;
                }
                total += execTime.getTimeTaken().toMillis();
            }
            System.out.println(" Num of " + i + " policies: " + total/(2) + " per policy: " +
                    (total/2)/((i+2)*119451.0));
        }
    }
}
