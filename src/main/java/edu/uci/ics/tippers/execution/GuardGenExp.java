package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.generation.policy.PolicyGroupGen;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardHit;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

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

    public GuardGenExp(){
        polper = new PolicyPersistor();
        pgg = new PolicyGroupGen();
        guardPersistor = new GuardPersistor();
    }


    public void generateGuards(){

        List<Integer> queriers = pgg.getNotLoners(pgg.MIN_GROUP_MEMBERSHIP);
        for(int querier: queriers) {
            System.out.println("Querier #: " + querier);
            List<BEPolicy> policies = polper.retrievePolicies(String.valueOf(querier), "user");
            BEExpression beExpression = new BEExpression(policies);

            Duration guardGen = Duration.ofMillis(0);
            Instant fsStart = Instant.now();
            GuardHit gh = new GuardHit(beExpression, true);
            Instant fsEnd = Instant.now();
            guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
            System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());

        }
    }


}
