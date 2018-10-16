package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.FactorExtension;
import edu.uci.ics.tippers.model.guard.FactorSelection;
import edu.uci.ics.tippers.model.guard.PolicyFilter;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class RunExp {

    List<BEPolicy> bePolicyList;

    public RunExp() {
        bePolicyList = new ArrayList<>();
    }


    public void readPolicies(String filePath){
        BEExpression beExpression = new BEExpression();
        beExpression.parseJSONList(Reader.readTxt(filePath));
        bePolicyList.addAll(beExpression.getPolicies());
    }

    /**
     * Returns a list with first element as the guard generation cost and the second
     * element as the guard execution cost
     * @param beExpression
     * @return
     */
    public List<Duration> generateGuard(BEExpression beExpression){

        List<Duration> guardCosts = new ArrayList<>();

        Duration guardGen = Duration.ofMillis(0);
        /** Extension **/
        FactorExtension f = new FactorExtension(beExpression);
        Instant feStart = Instant.now();
        f.doYourThing();
        Instant feEnd = Instant.now();
        guardGen = guardGen.plus(Duration.between(feStart, feEnd));

        /** Factorization **/
        FactorSelection gf = new FactorSelection(f.getGenExpression());
        Instant fsStart = Instant.now();
        gf.selectGuards();
        Instant fsEnd = Instant.now();
        guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));

        Duration guardRunTime = gf.computeGuardCosts();

        guardCosts.add(guardGen);
        guardCosts.add(guardRunTime);

        return guardCosts;
    }

    /**
     *
     * example: start_policies = 50, k = 4, rpq = 5, epochs = 64, fileName = policy114.json
     * returns a list with first element as guard generation cost and second as query evaluation
     * cost for the number of epochs
     *
     */
    public List<Duration> runExpt(int start_policies, int k, int rpq, int epochs, String fileName){

        Duration genTotal = Duration.ofMillis(0);
        Duration evalTotal = Duration.ofMillis(0);
        readPolicies(PolicyConstants.BE_POLICY_DIR + fileName);

        int i = start_policies;
        BEExpression current = new BEExpression(this.bePolicyList.subList(0,i));
        List<Duration> gCosts =  generateGuard(current);
        while (i < start_policies + epochs - 1){
            System.out.println("Guard Eval cost: " +  gCosts.get(1).toMillis());
//            evalTotal = evalTotal.plusMillis(k * rpq * gCosts.get(1).toMillis()); //Guard execution cost
            PolicyFilter pf = new PolicyFilter(new BEExpression(this.bePolicyList.subList(i,i+k)));
            evalTotal = evalTotal.plus(pf.computeCost()); //Policy Filter cost for period k
            System.out.println("Policy Filter: " + pf.computeCost());
            i += k;
            current = new BEExpression(this.bePolicyList.subList(0,i));
            gCosts = generateGuard(current);
            genTotal = genTotal.plusMillis(gCosts.get(0).toMillis()); //Guard Generation Cost
            System.out.println("Guard Gen" + gCosts.get(0).toMillis());
        }
        List<Duration> total_time = new ArrayList<>();
        total_time.add(genTotal);
        total_time.add(evalTotal);
        return total_time;
    }

    public static void main(String args[]) {
        int[] kValues = {12};
        int epochs = 64;
        int start_policies = 50;
        int rpq = 5;
        String fileName = "policy114.json";
        int numberOfReps = 5;
        TreeMap<String, Duration> runTimes = new TreeMap<>();
        RunExp re = new RunExp();
        for (int kValue : kValues) {
            List<Duration> gTimes = new ArrayList<>();
            Duration genTime = Duration.ofMillis(0);
            Duration queryTime = Duration.ofMillis(0);
            for (int i = 0; i < numberOfReps; i++) {
                gTimes = re.runExpt(start_policies, kValue, rpq, epochs, fileName);
                genTime = genTime.plus(gTimes.get(0));
                queryTime = queryTime.plus(gTimes.get(1));
            }
            runTimes.put(String.valueOf(kValue) + " Generation", Duration.ofMillis(genTime.toMillis()/numberOfReps));
            runTimes.put(String.valueOf(kValue) + " Execution", Duration.ofMillis(queryTime.toMillis()/numberOfReps));
        }
        Writer writer = new Writer();
        writer.createTextReport(runTimes, PolicyConstants.BE_POLICY_DIR);
    }
}
