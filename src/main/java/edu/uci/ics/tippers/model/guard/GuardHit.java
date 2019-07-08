package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class GuardHit {

    //Original expression
    Term input;
    Set<ObjectCondition> canFactors;
    List<Term> finalForm;

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();


    public GuardHit(BEExpression originalExp){

        this.input = new Term();
        this.input.setRemainder(originalExp);
        this.input.setQuotient(originalExp);
        finalForm = new ArrayList<>();

        this.canFactors = new HashSet<>();
        Set<ObjectCondition> pFactors = originalExp.getPolicies().stream()
                .flatMap(p -> p.getObject_conditions().stream())
                .filter(o -> PolicyConstants.INDEX_ATTRS.contains(o.getAttribute()))
                .collect(Collectors.toSet());
        for (ObjectCondition pf: pFactors) {
            Boolean match = false;
            for (ObjectCondition cf: canFactors) {
                if(pf.equalsWithoutId(cf)) match = true;
            }
            if(!match) canFactors.add(pf);
        }
        generateAllGuards(this.input);
    }


    private double benefit(ObjectCondition factor, BEExpression quotient){
        return quotient.estimateCostForTableScan() - quotient.estimateCostOfGuardRep(factor,false);
    }

    private double cost(ObjectCondition factor){
        return PolicyConstants.NUMBER_OR_TUPLES * factor.computeL() * PolicyConstants.IO_BLOCK_READ_COST;
    }


    private Term generateGuard(Term current){
        Set<ObjectCondition> removal = new HashSet<>();
        double maxUtility = 0.0;
        Term mTerm = null;
        for (ObjectCondition objectCondition : this.canFactors) {
            BEExpression tempQuotient = new BEExpression(current.getRemainder());
            tempQuotient.checkAgainstPolices(objectCondition);
            if (tempQuotient.getPolicies().size() > 1) { //was able to factorize
                double utility = benefit(objectCondition, tempQuotient)/cost(objectCondition);
                if (utility > maxUtility) { //factorized is better than original
                    maxUtility = utility;
                    mTerm = new Term();
                    mTerm.setQuotient(tempQuotient);
                    mTerm.setRemainder(new BEExpression(current.getRemainder()));
                    mTerm.getRemainder().getPolicies().removeAll(mTerm.getQuotient().getPolicies());
                    mTerm.setFactor(objectCondition);
                    mTerm.setBenefit(benefit(mTerm.getFactor(), mTerm.getQuotient()));
                    mTerm.setCost(cost(mTerm.getFactor()));
                    mTerm.setUtility(utility);
                }
            } else removal.add(objectCondition); //not a factor of at least two policies
        }
        this.canFactors.removeAll(removal);
        return mTerm;
    }

    private Term forSinglePolicy(BEPolicy bePolicy){
        double freq = PolicyConstants.NUMBER_OR_TUPLES;
        ObjectCondition gOC = new ObjectCondition();
        for (ObjectCondition oc : bePolicy.getObject_conditions()) {
            if (!PolicyConstants.INDEX_ATTRS.contains(oc.getAttribute())) continue;
            if (oc.computeL() < freq) {
                freq = oc.computeL();
                gOC = oc;
            }
        }
        Term rTerm = new Term();
        rTerm.setFactor(gOC);
        BEExpression quotient = new BEExpression();
        quotient.getPolicies().add(bePolicy);
        rTerm.setQuotient(quotient);
        return rTerm;
    }

    private void generateAllGuards(Term current) {
        while (true) {
            if (current.getRemainder().getPolicies().size() > 1) {
                if (canFactors.size() > 1) {
                    Term nTerm = generateGuard(current);
                    if (nTerm != null) {
//                        System.out.println("Guard generated" + nTerm.getFactor().print());
                        finalForm.add(nTerm);
                        current = nTerm;
                        continue;
                    }
                }
            }
            break;
        }
        for (BEPolicy bePolicy : current.getRemainder().getPolicies()) {
            finalForm.add(forSinglePolicy(bePolicy));
        }
    }

    public String createGuardedQuery(boolean noDuplicates){
        List<String> gList = createGuardQueries();
        StringBuilder queryExp = new StringBuilder();
        String delim = "";
        for (String g: gList) {
            queryExp.append(delim);
            queryExp.append(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + g);
            delim = noDuplicates ? PolicyConstants.UNION : PolicyConstants.UNION_ALL;
        }
        return queryExp.toString();
    }

    public List<String> createGuardQueries(){
        List<String> guardQueries = new ArrayList<>();
        for (Term mt: finalForm)
           guardQueries.add(mt.getFactor().print() + PolicyConstants.CONJUNCTION + mt.getQuotient().createQueryFromPolices());
        return  guardQueries;
    }

    private String createCleanQueryFromGQ(ObjectCondition guard, BEExpression partition) {
        StringBuilder query = new StringBuilder();
        query.append(guard.print());
        query.append(PolicyConstants.CONJUNCTION);
        query.append("(");
        partition.cleanQueryFromPolices();
        query.append(partition.createQueryFromPolices());
        query.append(")");
        return query.toString();
    }


    public List<String> guardAnalysis(int repetitions) {
        List<String> guardResults = new ArrayList<>();
        Duration totalEval = Duration.ofMillis(0);
        for (Term mt : finalForm) {
            System.out.println("Executing Guard " + mt.getFactor().print());
            StringBuilder guardString = new StringBuilder();
            guardString.append(mt.getQuotient().getPolicies().size());
            guardString.append(",");
            int numOfPreds = mt.getQuotient().getPolicies().stream().mapToInt(BEPolicy::countNumberOfPredicates).sum();
            guardString.append(numOfPreds);
            guardString.append(",");
            List<Long> gList = new ArrayList<>();
            List<Long> cList = new ArrayList<>();
            int gCount = 0, tCount = 0;
            for (int i = 0; i < repetitions; i++) {
                MySQLResult guardResult = mySQLQueryManager.runTimedQueryWithSorting(mt.getFactor().print());
                if (gCount == 0) gCount = guardResult.getResultCount();
                gList.add(guardResult.getTimeTaken().toMillis());
                MySQLResult completeResult = mySQLQueryManager.runTimedQueryWithSorting(createCleanQueryFromGQ(mt.getFactor(),
                        mt.getQuotient()));
                if (tCount == 0) tCount = completeResult.getResultCount();
                cList.add(completeResult.getTimeTaken().toMillis());

            }

            Duration gCost, gAndPcost;
            if(repetitions >= 3) {
                Collections.sort(gList);
                List<Long> clippedGList = gList.subList(1, repetitions - 1);
                gCost = Duration.ofMillis(clippedGList.stream().mapToLong(i -> i).sum() / clippedGList.size());
                Collections.sort(cList);
                List<Long> clippedCList = cList.subList(1, repetitions - 1);
                gAndPcost = Duration.ofMillis(clippedCList.stream().mapToLong(i -> i).sum() / clippedCList.size());
            }
            else{
                gCost =  Duration.ofMillis(gList.stream().mapToLong(i -> i).sum() / gList.size());
                gAndPcost = Duration.ofMillis(cList.stream().mapToLong(i -> i).sum() / cList.size());
            }

            guardString.append(gCount);
            guardString.append(",");
            guardString.append(tCount);
            guardString.append(",");

            double rCount = 0.0;
            if(tCount != 0 ){
                rCount = gCount / tCount;
            }
            guardString.append(rCount);
            guardString.append(",");

            guardString.append(mt.getBenefit());
            guardString.append(",");

            guardString.append(mt.getCost());
            guardString.append(",");

            guardString.append(mt.getUtility());
            guardString.append(",");

            guardString.append(gCost.toMillis());
            guardString.append(",");
            guardString.append(gAndPcost.toMillis());
            guardString.append(",");
            guardString.append(createCleanQueryFromGQ(mt.getFactor(), mt.getQuotient()));
            guardResults.add(guardString.toString());
            totalEval = totalEval.plus(gAndPcost);
        }
        System.out.println("Total Guard Evaluation time: " + totalEval);
        guardResults.add("Total Guard Evaluation time," + totalEval.toMillis());
        return guardResults;

    }

}
