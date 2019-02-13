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

public class FactorSearch {

    //Original expression
    Term input;
    PriorityQueue<Term> open;
    HashSet<Term> closed;
    HashMap<Term, Term> parentMap;
    HashMap<Term, Double> scores;
    int depth;
    int maxDepth;
    Set<ObjectCondition> canFactors;
    double epsilon;
    Map<ObjectCondition, BEExpression> gMap;
    MySQLQueryManager mySQLQueryManager;


    public FactorSearch(BEExpression input){
        this.input = new Term();
        this.input.setRemainder(input);
        this.input.setFactor(input.getPolicies().get(0).getObject_conditions().get(0));
        this.input.setQuotient(input);
        this.input.setFscore(Double.POSITIVE_INFINITY);
        this.open = new PriorityQueue<>();
        this.open.add(this.input);
        this.scores = new HashMap<Term, Double>();
        this.closed = new HashSet<Term>();
        this.parentMap = new HashMap<Term, Term>();
        this.depth = 0;
        this.maxDepth = (int) Math.round(Math.log(input.getPolicies().size())/Math.log(2.0));
        this.canFactors = new HashSet<>();
        Set<ObjectCondition> pFactors = input.getPolicies().stream()
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
        this.epsilon = 1.0;
        this.gMap = new HashMap<>();
        this.mySQLQueryManager = new MySQLQueryManager();
    }

    private boolean utility(BEExpression original, ObjectCondition factor, boolean costEvalOnly){
        double beforeCost = original.estimateCostForSelection(costEvalOnly);
        double afterCost = original.estimateCostOfGuardRep(factor, costEvalOnly);
        return beforeCost > afterCost;
    }

    private double superPolicyCost(BEExpression remainder) {
        Map<String, List<ObjectCondition>> aMap = new HashMap<>();
        for (int i = 0; i < remainder.getPolicies().size(); i++) {
            BEPolicy pol = remainder.getPolicies().get(i);
            for (int j = 0; j < pol.getObject_conditions().size(); j++) {
                ObjectCondition oc = pol.getObject_conditions().get(j);
                if(aMap.containsKey(oc.getAttribute())) {
                    if((oc.getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR) ||
                            (oc.getAttribute().equalsIgnoreCase(PolicyConstants.ACTIVITY_ATTR) ||
                                    oc.getAttribute().equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR)))){
                        aMap.get(oc.getAttribute()).add(oc);
                    }
                    else{
                        ObjectCondition sup = aMap.get(oc.getAttribute()).get(0).union(oc);
                        aMap.get(oc.getAttribute()).set(0, sup);
                    }
                }
                else{
                    List<ObjectCondition> ocs = new ArrayList<>();
                    ocs.add(oc);
                    aMap.put(oc.getAttribute(), ocs);
                }
            }
        }
        double lowSel= 1.0;
        for (String attr: aMap.keySet()) {
            double sel;
            if (aMap.get(attr).size() > 1) {
                sel = 1.0;
                for (int i = 0; i < aMap.get(attr).size(); i++ ){
                    sel *= (1 - aMap.get(attr).get(i).computeL());
                }
                sel = 1 - sel;
            }
            else{
                sel = aMap.get(attr).get(0).computeL();
            }
            if (sel < lowSel) lowSel = sel;
        }
        return PolicyConstants.NUMBER_OR_TUPLES * lowSel *(PolicyConstants.IO_BLOCK_READ_COST  +
                PolicyConstants.ROW_EVALUATE_COST * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED *
                        aMap.keySet().size());
    }

    private List<Term> factorize(BEExpression toFactorize){
        Set<ObjectCondition> removal = new HashSet<>();
        List<Term> treeNodes = new ArrayList<Term>();
        for (ObjectCondition objectCondition : this.canFactors) {
            BEExpression tempQuotient = new BEExpression(toFactorize);
            tempQuotient.checkAgainstPolices(objectCondition);
            if (tempQuotient.getPolicies().size() > 1) { //was able to factorize
                if (utility(tempQuotient, objectCondition, false)) { //factorized is better than original
                    Term canTerm = new Term();
                    canTerm.setQuotient(tempQuotient);
                    canTerm.setRemainder(new BEExpression(toFactorize));
                    canTerm.getRemainder().getPolicies().removeAll(canTerm.getQuotient().getPolicies());
                    canTerm.setFactor(objectCondition);
                    canTerm.setGscore(canTerm.getQuotient().estimateCostOfGuardRep(objectCondition, false));
                    canTerm.setHscore(superPolicyCost(canTerm.getRemainder()));
                    canTerm.setFscore(canTerm.getGscore() + canTerm.getHscore());
                    treeNodes.add(canTerm);
                }
            } else removal.add(objectCondition); //not a factor of at least two policies
        }
        this.canFactors.removeAll(removal);
        return treeNodes;
    }

    /**
     * Returns the final factorized expression through A-star search
     */
    public void search(){
        Term current;
        while(!this.open.isEmpty()) {
            current = this.open.remove();
            if(!this.closed.contains(current)){
                this.closed.add(current);
                List<Term> candidates = factorize(current.getRemainder());
                if(candidates.isEmpty()) {
                    createQueryMap(current); //Factorization complete
                }
                this.depth += 1;
                for (Term c: candidates) {
                    if(!this.closed.contains(c)){
                        if(this.open.contains(c)){
                            if(this.scores.get(c) > c.getFscore()){
                                this.open.remove(c); //Remove the old node
                            }
                            else continue;
                        }
                        this.open.add(c);
                        this.parentMap.put(c, current);
                        this.scores.put(c, c.getFscore());
                    }
                }
                for (Term oc: open) {
                    oc.setFscore(oc.getGscore() + oc.getHscore()
                            * (1 + this.epsilon) - ((epsilon * this.depth)/this.maxDepth));
                    this.scores.replace(oc, oc.getFscore());
                }
            }
        }
    }



    private void createQueryMap(Term current){
        //for remainder with no guard
        for (BEPolicy bePolicy : current.getRemainder().getPolicies()) {
            double freq = PolicyConstants.NUMBER_OR_TUPLES;
            ObjectCondition gOC = new ObjectCondition();
            for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                if (!PolicyConstants.INDEX_ATTRS.contains(oc.getAttribute())) continue;
                if (oc.computeL() < freq) {
                    freq = oc.computeL();
                    gOC = oc;
                }
            }
            BEExpression quo = new BEExpression();
            quo.getPolicies().add(bePolicy);
            gMap.put(gOC, quo);
        }
        //for the factorized expression
        while(true){
            gMap.put(current.getFactor(), current.getQuotient());
            Term parent = parentMap.get(current);
            if(parent.getFscore() == Double.POSITIVE_INFINITY) break;
            current = parent;
        }
    }


    public List<String> printDetailedResults() {
        List<String> guardResults = new ArrayList<>();
        int repetitions = 3;
        Duration totalEval = Duration.ofMillis(0);
        for (ObjectCondition kOb : gMap.keySet()) {
            System.out.println("Executing Guard " + kOb.print());
            StringBuilder guardString = new StringBuilder();
            guardString.append(gMap.get(kOb).getPolicies().size());
            guardString.append(",");
            int numOfPreds = gMap.get(kOb).getPolicies().stream().mapToInt(BEPolicy::countNumberOfPredicates).sum();
            guardString.append(numOfPreds);
            guardString.append(",");
            List<Long> gList = new ArrayList<>();
            List<Long> cList = new ArrayList<>();
            int gCount = 0, tCount = 0;
            for (int i = 0; i < repetitions; i++) {
                MySQLResult guardResult = mySQLQueryManager.runTimedQueryWithResultCount(kOb.print());
                if (gCount == 0) gCount = guardResult.getResultCount();
                gList.add(guardResult.getTimeTaken().toMillis());
                MySQLResult completeResult = mySQLQueryManager.runTimedQueryWithResultCount(new Term(kOb, gMap.get(kOb)).printFQ());
                if (tCount == 0) tCount = completeResult.getResultCount();
                cList.add(completeResult.getTimeTaken().toMillis());

            }
            Collections.sort(gList);
            List<Long> clippedGList = gList.subList(1, repetitions - 1);
            Duration gCost = Duration.ofMillis(clippedGList.stream().mapToLong(i -> i).sum() / clippedGList.size());
            Collections.sort(cList);
            List<Long> clippedCList = cList.subList(1, repetitions - 1);
            Duration gAndPcost = Duration.ofMillis(clippedCList.stream().mapToLong(i -> i).sum() / clippedCList.size());

            guardString.append(gCount);
            guardString.append(",");
            guardString.append(tCount);
            guardString.append(",");

            guardString.append(gCost.toMillis());
            guardString.append(",");
            guardString.append(gAndPcost.toMillis());
            guardString.append(",");
            guardString.append(new Term(kOb, gMap.get(kOb)).printFQ());
            guardResults.add(guardString.toString());
            totalEval = totalEval.plus(gAndPcost);
        }
        System.out.println("Total Guard Evaluation time: " + totalEval);
        guardResults.add("Total Guard Evaluation time," + totalEval.toMillis());
        return guardResults;
    }

}
