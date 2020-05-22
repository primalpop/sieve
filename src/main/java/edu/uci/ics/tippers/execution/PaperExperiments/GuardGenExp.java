package edu.uci.ics.tippers.execution.PaperExperiments;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyGen;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;


/**
 * Experiment for measuring the time time taken for generating guards belonging to queriers
 * of different policy selectivities.
 * Experiment 1.1 in the paper
 */
public class GuardGenExp {

    PolicyPersistor polper;
    GuardPersistor guardPersistor;
    Connection connection;

    public GuardGenExp(){
        this.polper = new PolicyPersistor();
        this.guardPersistor = new GuardPersistor();
        this.connection = MySQLConnectionManager.getInstance().getConnection();
    }

    private void writeExecTimes(int querier, int policyCount, int timeTaken){
            String execTimesInsert = "INSERT INTO gg_results (querier, pCount, timeTaken) VALUES (?, ?, ?)";
            try {
                PreparedStatement eTStmt = connection.prepareStatement(execTimesInsert);
                eTStmt.setInt(1, querier);
                eTStmt.setInt(2, policyCount);
                eTStmt.setInt(3, timeTaken);
                eTStmt.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    public void generateGuards(List<Integer> queriers){
        boolean first = true;
        for(int querier: queriers) {
            List<BEPolicy> allowPolicies = polper.retrievePolicies(String.valueOf(querier),
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            if(allowPolicies == null) continue;
            System.out.println("Querier #: " + querier + " with " + allowPolicies.size() + " allow policies");
            BEExpression allowBeExpression = new BEExpression(allowPolicies);
            Duration guardGen = Duration.ofMillis(0);
            Instant fsStart = Instant.now();
            SelectGuard gh = new SelectGuard(allowBeExpression, true, PolicyConstants.WIFI_DBH_ATTR_LIST,
                    PolicyConstants.WIFI_DBH_RANGE_ATTR_LIST, PolicyConstants.WIFI_DBH_ATTRIBUTE_IND);
            Instant fsEnd = Instant.now();
            System.out.println(gh.createGuardedQuery(true));
            guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
            System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());
            guardPersistor.insertGuard(gh.create(String.valueOf(querier), "user"));
            if(!first) writeExecTimes(querier, allowPolicies.size(), (int) guardGen.toMillis());
            else first = false;
        }
    }

    public static void main(String [] args){
        GuardGenExp ge = new GuardGenExp();
        PolicyGen pg = new PolicyGen();
        List<Integer> users = pg.getAllUsers(true);
        ge.generateGuards(users);
    }
}
