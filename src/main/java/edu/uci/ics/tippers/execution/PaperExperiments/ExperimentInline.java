package edu.uci.ics.tippers.execution.PaperExperiments;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.QueryResult;


public class ExperimentInline {


    public ExperimentInline(){

    }

    /**
     * Experiment to determine the policy evaluation cost
     * TODO: Isolate read_cost and eval_cost, the current constant for policy_eval includes read_cost as well
     * @param args
     */
    public static void main(String[] args){
        MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();
        String query_predicates = ") Select * from polEval";
        String querier = "7073";
        String guard_id = "'a21c9919-6e54-4b02-90d8-cc80720833a9'";
        String guard = "WITH polEval as (Select * from PRESENCE WHERE (start_date<= \"2018-02-05\") " +
                "AND (start_date>= \"2018-02-03\")";
        String udf_query = " hybcheck(" + querier + ", "
                 + guard_id +  " , user_id, location_id, start_date, " +
                        "start_time, user_profile, user_group ) = 1";
        QueryResult udfTime = mySQLQueryManager.runTimedQueryExp(guard + PolicyConstants.CONJUNCTION
                + udf_query + query_predicates, 3);
        System.out.println("UDF time " + udfTime.getTimeTaken().toMillis());
        String add_policy_1 = "((user_profile= \"faculty\") " +
                "AND (start_date<= \"2018-04-30\") AND (start_date>= \"2018-02-28\") " +
                "AND (start_time>= \"00:10:00\") AND (start_time<= \"10:59:00\"))";
        String add_policy_2 = "((user_profile= \"graduate\") " +
                "AND (location_id = \"3145-clwa-5231\")" +
                "AND (start_date<= \"2018-03-30\") AND (start_date>= \"2018-02-15\") " +
                "AND (start_time>= \"15:12:00\") AND (start_time<= \"23:59:00\"))";

        for (int i = 8; i < 200; i=i+10) {
            String query = guard + " AND ((user_profile= \"graduate\") " +
                    "AND (start_date<= \"2018-05-01\") AND (start_date>= \"2018-02-28\") " +
                    "AND (start_time>= \"08:00:00\") AND (start_time<= \"17:00:00\")) " +
                    "OR ((location_id = \"3146-clwa-6217\") AND (start_date<= \"2018-01-30\") " +
                    "AND (start_date>= \"2018-02-01\") AND (start_time>= \"12:00:00\") AND (start_time<= \"17:00:00\"))  ";
            for (int j = 0; j < i; j++)
                if(j%2 == 0)
                    query += PolicyConstants.DISJUNCTION + add_policy_1;
                else
                    query += PolicyConstants.DISJUNCTION + add_policy_2;
            query += query_predicates;
            QueryResult execTime = mySQLQueryManager.runTimedQueryExp(query, 3);
            System.out.println(" Num of " + i + " policies: " + execTime.getTimeTaken().toMillis());
        }
    }
}
