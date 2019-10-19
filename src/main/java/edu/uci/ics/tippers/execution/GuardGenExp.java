package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.generation.policy.PolicyGroupGen;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardHit;
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
 */
public class GuardGenExp {

    PolicyPersistor polper;
    PolicyGroupGen pgg;
    GuardPersistor guardPersistor;
    Connection connection;

    public GuardGenExp(){
        this.polper = new PolicyPersistor();
        this.pgg = new PolicyGroupGen();
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

    public void generateGuards(){
        List<Integer> queriers = pgg.getNotLoners(1);
        for(int querier: queriers) {
            List<BEPolicy> policies = polper.retrievePolicies(String.valueOf(querier), "user");
            BEExpression beExpression = new BEExpression(policies);
            System.out.println("Querier #: " + querier + " with " + policies.size() + " policies");
            Duration guardGen = Duration.ofMillis(0);
            Instant fsStart = Instant.now();
            GuardHit gh = new GuardHit(beExpression, true);
            Instant fsEnd = Instant.now();
            guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
            System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());
            guardPersistor.insertGuard(gh.create(querier, "user"));
            writeExecTimes(querier, beExpression.getPolicies().size(), (int) guardGen.toMillis());
        }
    }

    public static void main(String [] args){
        GuardGenExp ge = new GuardGenExp();
        ge.generateGuards();
    }
}
