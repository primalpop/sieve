package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

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
                .filter(o -> PolicyConstants.INDEXED_ATTRS.contains(o.getAttribute()))
                .collect(Collectors.toSet());
        for (ObjectCondition pf: pFactors) {
            Boolean match = false;
            for (ObjectCondition cf: canFactors) {
                if(pf.equalsWithoutId(cf)) match = true;
            }
            if(!match) canFactors.add(pf);
        }

    }

    private boolean utility(BEExpression original, ObjectCondition factor, boolean costEvalOnly){
        double beforeCost = original.estimateCostForSelection(costEvalOnly);
        double afterCost = original.estimateCostOfGuardRep(factor, costEvalOnly);
        return beforeCost > afterCost;
    }

    private double superPolicyCost(BEExpression remainder) {
        Map<String, ObjectCondition> aMap = new HashMap<>();
        for (int i = 0; i < remainder.getPolicies().size(); i++) {
            BEPolicy pol = remainder.getPolicies().get(i);
            for (int j = 0; j < pol.getObject_conditions().size(); j++) {
                ObjectCondition oc = pol.getObject_conditions().get(j);
                if(aMap.containsKey(oc.getAttribute())) {
                    ObjectCondition sup = aMap.get(oc.getAttribute()).union(oc);
                    aMap.put(oc.getAttribute(), sup);
                }
                else
                    aMap.put(oc.getAttribute(), oc);
            }
        }
        BEPolicy superPolicy = new BEPolicy(new ArrayList<ObjectCondition>(aMap.values()));
        return superPolicy.estimateCost(false);
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
    public String search(){
        Term current;
        while(!this.open.isEmpty()) {
            current = this.open.remove();
            if(!this.closed.contains(current)){
                this.closed.add(current);
                List<Term> candidates = factorize(current.getRemainder());
                if(candidates.isEmpty()) {
                    return createQuery(current); //Factorization complete
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
            }
        }
        return null;
    }

    private String createQuery(Term current){
        StringBuilder query = new StringBuilder();
        query.append("(");
        query.append(current.getRemainder().createQueryFromPolices());
        query.append(")");
        while(true){
            query.append(PolicyConstants.DISJUNCTION);
            query.append("(");
            query.append(current.printFQ());
            query.append(")");
            Term parent = parentMap.get(current);
            if(parent.getFscore() == Double.POSITIVE_INFINITY) break;
            current = parent;
        }
        return query.toString();
    }


}
