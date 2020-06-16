package edu.uci.ics.tippers.model.guard.deprecated;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.dbms.QueryResult;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.guard.Term;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class FactorSearch {

    //Original expression
    Term input;
    Term finalTerm;
    PriorityQueue<Term> open;
    HashSet<Term> closed;
    HashMap<Term, Term> parentMap;
    HashMap<Term, Double> scores;
    Set<ObjectCondition> canFactors;
    double epsilon;
    double totalCost;
    QueryManager queryManager;


    public FactorSearch(BEExpression originalExp ){
        this.input = new Term();
        this.input.setRemainder(originalExp);
        this.input.setFactor(originalExp.getPolicies().get(0).getObject_conditions().get(0));
        this.input.setQuotient(originalExp);
        this.input.setFscore(Double.POSITIVE_INFINITY);
        this.open = new PriorityQueue<>();
        this.open.add(this.input);
        this.scores = new HashMap<Term, Double>();
        this.closed = new HashSet<Term>();
        this.parentMap = new HashMap<Term, Term>();
        this.canFactors = new HashSet<>();
        Set<ObjectCondition> pFactors = originalExp.getPolicies().stream()
                .flatMap(p -> p.getObject_conditions().stream())
                .filter(o -> PolicyConstants.ATTRIBUTES.contains(o.getAttribute()))
                .collect(Collectors.toSet());
        for (ObjectCondition pf: pFactors) {
            Boolean match = false;
            for (ObjectCondition cf: canFactors) {
                if(pf.equalsWithoutId(cf)) match = true;
            }
            if(!match) canFactors.add(pf);
        }
        this.epsilon = 1.0;
        this.totalCost = originalExp.getPolicies().stream().mapToDouble(BEPolicy::getEstCost).sum();
        this.queryManager = new QueryManager();
        this.finalTerm = null;
    }

    private boolean utility(BEExpression original, ObjectCondition factor, boolean costEvalOnly){
        double beforeCost = original.estimateCostForSelection(costEvalOnly);
        double afterCost = original.estimateCostOfGuardRep(factor, costEvalOnly);
        return beforeCost > afterCost;
    }

    /**
     * Computing h-score based on the
     * lower bound of the cost of evaluating remainder based on super policy
     * @param remainder
     * @return
     */
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
        return PolicyConstants.getNumberOfTuples() * lowSel *(PolicyConstants.IO_BLOCK_READ_COST  +
                PolicyConstants.ROW_EVALUATE_COST * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED *
                        aMap.keySet().size());
    }



    private List<Term> factorize(Term current){
        Set<ObjectCondition> removal = new HashSet<>();
        List<Term> treeNodes = new ArrayList<Term>();
        for (ObjectCondition objectCondition : this.canFactors) {
            BEExpression tempQuotient = new BEExpression(current.getRemainder());
            tempQuotient.checkAgainstPolices(objectCondition);
            if (tempQuotient.getPolicies().size() > 1) { //was able to factorize
                if (utility(tempQuotient, objectCondition, false)) { //factorized is better than original
                    Term canTerm = new Term();
                    canTerm.setQuotient(tempQuotient);
                    canTerm.setRemainder(new BEExpression(current.getRemainder()));
                    canTerm.getRemainder().getPolicies().removeAll(canTerm.getQuotient().getPolicies());
                    canTerm.setFactor(objectCondition);
                    canTerm.setGscore(current.getGscore() + canTerm.getQuotient().estimateCostOfGuardRep(objectCondition, false));
                    canTerm.setHscore(this.totalCost - canTerm.getGscore());
                    canTerm.setFscore(canTerm.getGscore() + canTerm.getHscore());
                    treeNodes.add(canTerm);
                }
            } else removal.add(objectCondition); //not a factor of at least two policies
        }
        this.canFactors.removeAll(removal);
        return treeNodes;
    }

    private int countDepth(Term node){
        int depth = 0;
        Term current = node;
        while(true){
            Term parent = parentMap.get(current);
            if(parent == null) break;
            current = parent;
            depth+= 1;
        }
        return depth;
    }

    /**
     * Returns the final factorized expression through A-star search
     */
    public void search(int depthAllowed){
        Term current;
        int depth = 0;
        while(!this.open.isEmpty()) {
            current = this.open.remove();
            if(!this.closed.contains(current)){
                this.closed.add(current);
                List<Term> candidates = factorize(current);
                if(candidates.isEmpty() || depth > depthAllowed) {
                    this.finalTerm = current; //Factorization complete
                    break;
                }
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
                    depth = countDepth(oc);
                    if(depth > 0) {
                        oc.setFscore(oc.getGscore() + oc.getHscore()
                                * ((1 + this.epsilon) - ((epsilon * depth) / depthAllowed)));
                        this.scores.replace(oc, oc.getFscore());
                    }
                }
            }
        }
    }

    /**
     * Creates the complete guarded expression query string with UNION or UNION ALL based on the boolean flag
     * @return
     */
    public String createGuardedExpQuery(boolean noDuplicates){
        List<String> gList = createGuardQueries();
        StringBuilder queryExp = new StringBuilder();
        String delim = "";
        for (String g: gList) {
            queryExp.append(delim);
            queryExp.append(PolicyConstants.SELECT_ALL + g);
            delim = noDuplicates ? PolicyConstants.UNION : PolicyConstants.UNION_ALL;
        }
        return queryExp.toString();
    }


    public GuardExp create(String querier, String querier_type){
        List<GuardPart> gps = new ArrayList<>();
        if(finalTerm.getRemainder().getPolicies().size() > 0) {
            for (BEPolicy bePolicy : finalTerm.getRemainder().getPolicies()) {
                double freq = PolicyConstants.getNumberOfTuples();
                ObjectCondition gOC = new ObjectCondition();
                for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                    if (!PolicyConstants.ATTRIBUTES.contains(oc.getAttribute())) continue;
                    if (oc.computeL() < freq) {
                        freq = oc.computeL();
                        gOC = oc;
                    }
                }
                bePolicy.cleanDuplicates();
                GuardPart gp = new GuardPart();
                gp.setGuard(gOC);
//                gp.setGuardPartition(new BEExpression()); TODO: to be fixed
                gps.add(gp);
            }
        }

        while(true){
            BEExpression quotientExp = new BEExpression(finalTerm.getQuotient());
            quotientExp.removeFromPolicies(finalTerm.getFactor());
            FactorSelection gf = new FactorSelection(finalTerm.getQuotient());
            gf.selectGuards(true);
            String quotientQuery = finalTerm.getFactor().print() +
                    PolicyConstants.CONJUNCTION + "(" + gf.createQueryFromExactFactor() + ")";
            GuardPart gp = new GuardPart();
            gp.setGuard(finalTerm.getFactor());
            gp.setGuardPartition(gf.getExpression());
            gps.add(gp);
            Term parent = parentMap.get(finalTerm);
            if(parent.getFscore() == Double.POSITIVE_INFINITY) break;
            finalTerm = parent;
        }
        GuardExp guardExp = new GuardExp();
        guardExp.setGuardParts(gps);
        guardExp.setAction(finalTerm.getQuotient().getPolicies().get(0).getAction());
        guardExp.setPurpose(finalTerm.getQuotient().getPolicies().get(0).getPurpose());
        guardExp.setQuerier(querier);
        guardExp.setQuerier_type(querier_type);
        guardExp.setDirty("false");
        guardExp.setLast_updated(new Timestamp(new Date().getTime()));
        return guardExp;
    }

    public List<String> createGuardQueries(){
        List<String> guardList = new ArrayList<>();
        //for remainder with no guard
        int guardCount = 0;
        if(finalTerm.getRemainder().getPolicies().size()>0){
            for (BEPolicy bePolicy : finalTerm.getRemainder().getPolicies()) {
                double freq = PolicyConstants.getNumberOfTuples();
                ObjectCondition gOC = new ObjectCondition();
                for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                    if (!PolicyConstants.ATTRIBUTES.contains(oc.getAttribute())) continue;
                    if (oc.computeL() < freq) {
                        freq = oc.computeL();
                        gOC = oc;
                    }
                }
                BEPolicy remPolicy = new BEPolicy(bePolicy);
                remPolicy.cleanDuplicates();
                String remainderQuery = gOC.print() +
                        PolicyConstants.CONJUNCTION + "(" + remPolicy.createQueryFromObjectConditions() + ")";
                guardList.add(remainderQuery);
                guardCount+= 1;
            }
            System.out.println("Remainder Guard count: " + guardCount);
        }
         Term node = finalTerm;
        //for the factorized expression
        while(true){
            Term parent = parentMap.get(node);
            if(parent == null) break;
            //Factorizing the quotient expression once
            BEExpression quotientExp = new BEExpression(node.getQuotient());
            quotientExp.removeFromPolicies(node.getFactor());
            FactorSelection gf = new FactorSelection(node.getQuotient());
            gf.selectGuards(true);
            String quotientQuery = node.getFactor().print() +
                    PolicyConstants.CONJUNCTION + "(" +  gf.createQueryFromExactFactor() + ")";
            guardList.add(quotientQuery);
            node = parent;
        }
        return guardList;
    }



    public List<String> printDetailedResults(int repetitions) {
        List<String> guardResults = new ArrayList<>();
        Duration totalEval = Duration.ofMillis(0);
        List <String> guardList = createGuardQueries();
        for (String kOb : guardList) {
            System.out.println("Executing Guard Expression: " + kOb);
            StringBuilder guardString = new StringBuilder();
            List<Long> gList = new ArrayList<>();
            List<Long> cList = new ArrayList<>();
            int gCount = 0, tCount = 0;
            for (int i = 0; i < repetitions; i++) {
                QueryResult guardResult = queryManager.runTimedQueryWithOutSorting(kOb, true);
                if (gCount == 0) gCount = guardResult.getResultCount();
                gList.add(guardResult.getTimeTaken().toMillis());
                QueryResult completeResult = queryManager.runTimedQueryWithOutSorting(kOb, true);
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

            guardString.append(gCost.toMillis());
            guardString.append(",");
            guardString.append(gAndPcost.toMillis());
            guardString.append(",");
            guardString.append(kOb);
            guardResults.add(guardString.toString());
            totalEval = totalEval.plus(gAndPcost);
        }
        System.out.println("Total Guard Evaluation time: " + totalEval);
        guardResults.add("Total Guard Evaluation time," + totalEval.toMillis());
        return guardResults;
    }
}
