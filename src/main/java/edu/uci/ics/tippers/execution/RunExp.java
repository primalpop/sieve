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
        int ext = f.doYourThing();
        System.out.println("Number of extensions: " + ext);
        Instant feEnd = Instant.now();
        guardGen = guardGen.plus(Duration.between(feStart, feEnd));

        /** Factorization **/
        FactorSelection gf = new FactorSelection(f.getGenExpression());
        Instant fsStart = Instant.now();
        gf.selectGuards();
        Instant fsEnd = Instant.now();
        guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
        System.out.println("Number of guards: " + gf.getIndexFilters().size());

        Duration guardRunTime = gf.computeGuardCosts();

        System.out.println("Guard " + gf.createQueryFromExactFactor());

        guardCosts.add(guardGen);
        guardCosts.add(guardRunTime);


        return guardCosts;
    }

    /**
     * For each epoch, returns the filter cost, guard cost and guard generation cost
     * Encoding: 1 - filter cost 2 - guard cost and 3 - guard generation cost
     * e.g., 1.1 - filter cost of 1st epoch, 3.7 - guard generation cost of 7th epoch
     * @param start_policies
     * @param k
     * @param rqp
     * @param epochs
     * @param fileName
     * @return
     */
    public TreeMap<String, Duration> runDetailedExpt(int start_policies, int k, int rqp, int epochs, String fileName){

        TreeMap<String, Duration> runTimes = new TreeMap<>();

        readPolicies(PolicyConstants.BE_POLICY_DIR + fileName);

        int i = start_policies;
        BEExpression current = new BEExpression(this.bePolicyList.subList(0,i));
        int counter = 0;
        List<Duration> gCosts =  generateGuard(current);
        String epoch = "";
        epoch += "2." + String.valueOf(i-start_policies);
        runTimes.put(epoch, gCosts.get(1));
        epoch = "";
        while (i < start_policies + epochs - 1){
            counter += 1;
            i+=1;
            if(counter == k){
                current = new BEExpression(this.bePolicyList.subList(0,i));
                gCosts = generateGuard(current);
                epoch += "3." + String.valueOf(i-start_policies);
                runTimes.put(epoch, gCosts.get(0));
                epoch = "";
                System.out.println("Guard generation cost at epoch" + String.valueOf(i-start_policies) + " :" + gCosts.get(0).toMillis());
                counter = 0;
            }
            Duration pfTime;
            if(counter != 0) {
                PolicyFilter pf = new PolicyFilter(new BEExpression(this.bePolicyList.subList(i - counter, i)));
                pfTime = pf.computeCost();
                epoch += "1." + String.valueOf(i-start_policies);
                runTimes.put(epoch, pfTime);
                epoch = "";
                System.out.println("Filter cost at epoch" + String.valueOf(i-start_policies) + ": " + pfTime.toMillis());
                System.out.println("Filter Selectivity " + String.valueOf(i-start_policies) + ": " + pf.getExpression().computeL());
            }
            System.out.println("Guard Cost at epoch" + String.valueOf(i-start_policies) + ": " + gCosts.get(1).toMillis());
            epoch += "2." + String.valueOf(i-start_policies);
            runTimes.put(epoch, gCosts.get(1));
            epoch = "";
        }
        return runTimes;
    }


    /**
     *
     * example: start_policies = 50, k = 4, rqp = 5, epochs = 64, fileName = policy114.json
     * returns a list with first element as guard generation cost and second as query evaluation
     * cost for the number of epochs
     *
     */
    public List<Duration> runExpt(int start_policies, int k, int rqp, int epochs, String fileName){

        System.out.println("============= Starting new k = " + k + " =================");
        readPolicies(PolicyConstants.BE_POLICY_DIR + fileName);

        int i = start_policies;
        BEExpression current = new BEExpression(this.bePolicyList.subList(0,i));
        int counter = 0;
        List<Duration> gCosts =  generateGuard(current);
        Duration genCost = Duration.ofMillis(0);
        Duration evalCost = Duration.ofMillis(0);
        Duration filterCost = Duration.ofMillis(0);
        Duration guardCost = Duration.ofMillis(0);
        evalCost = evalCost.plusMillis(gCosts.get(1).toMillis() * rqp);
        System.out.println("Guard Cost before epochs " + gCosts.get(1).toMillis());
        while (i < start_policies + epochs - 1){
            counter += 1;
            i+=1;
            if(counter == k){
                current = new BEExpression(this.bePolicyList.subList(0,i));
                gCosts = generateGuard(current);
                genCost = genCost.plusMillis(gCosts.get(0).toMillis());
                System.out.println("Guard regenerated: " + i );
                System.out.println("Guard generation cost: " + gCosts.get(0).toMillis());
                counter = 0;
            }
            Duration pfTime = Duration.ofMillis(0);
            if(counter != 0) {
                PolicyFilter pf = new PolicyFilter(new BEExpression(this.bePolicyList.subList(i - counter, i)));
                pfTime = pf.computeCost();
                System.out.println("Filter cost " + i + ": " + pfTime.toMillis());
                filterCost = filterCost.plusMillis(pfTime.toMillis());
            }
            System.out.println("Guard Cost " + i + ": " + gCosts.get(1).toMillis());
            guardCost = guardCost.plusMillis(gCosts.get(1).toMillis());
            evalCost = evalCost.plusMillis((pfTime.toMillis() + gCosts.get(1).toMillis())*rqp);
        }
        System.out.println("Total Filter Cost: " + filterCost.toMillis());
        System.out.println("Total Guard Cost: " + guardCost.toMillis());
        System.out.println("** Total Evaluation Cost: **" + evalCost.toMillis());
        System.out.println("** Total Guard Generation Cost: **" + genCost.toMillis());
        List<Duration> total_time = new ArrayList<>();
        total_time.add(genCost);
        total_time.add(evalCost);
        return total_time;
    }

    public static void main(String args[]) {
        int[] kValues = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
        int epochs = 15;
        int start_policies = 20;
        int rpq = 2;
        String[] fileNames = {"policy35.json"};
        List<Duration> times = new ArrayList<>();
        TreeMap<String, String> runTimes = new TreeMap<>();
        RunExp re = new RunExp();
        Writer writer = new Writer();
        for (int kValue : kValues) {
            times = re.runExpt(start_policies, kValue, rpq, epochs, fileNames[0]);
            runTimes.put( kValue + " generation", String.valueOf(times.get(0).toMillis()));
            runTimes.put( kValue + " evaluation", String.valueOf(times.get(1).toMillis()));
            writer.appendToCSVReport(runTimes, PolicyConstants.BE_POLICY_DIR, "result" + ".csv");
            runTimes.clear();
        }
    }
}
