package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.davidmoten.guavamini.Lists;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by cygnus on 11/1/17.
 */
public class BEExpression{

    List<BEPolicy> policies;

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

    public BEExpression(){
        this.policies = new ArrayList<BEPolicy>();
    }

    public BEExpression(List<BEPolicy> policies){
        this.policies = new ArrayList<BEPolicy>();
        for(BEPolicy bp: policies){
            this.policies.add(new BEPolicy(bp));
        }
    }


    public BEExpression(BEExpression beExpression){
        this.policies = new ArrayList<BEPolicy>(beExpression.getPolicies().size());
        for(BEPolicy bp: beExpression.getPolicies()){
            this.policies.add(new BEPolicy(bp));
        }
    }

    public List<BEPolicy> getPolicies() {
        return this.policies;
    }

    public void setPolicies(List<BEPolicy> policies) {
        this.policies = policies;
    }

    /**
     * Get a list of attributes from the policies in the expression
     * @return
     */
    public List<String> getPolAttributes(){
        Set<String> attrs = new HashSet<>();
        for(BEPolicy bp: policies) {
            attrs.addAll(bp.retrieveObjCondAttributes());
        }
        return Lists.newArrayList(attrs);
    }


    /**
     * Get all the distinct object conditions from a list of policies
     * @return
     */
    public List<ObjectCondition> getPolObjCond(){
        Set<ObjectCondition> distinctObjCond = new HashSet<>();
        for (int i = 0; i < this.policies.size(); i++) {
            List<ObjectCondition> policyCond = this.policies.get(i).getObject_conditions();
            for (int j = 0; j < policyCond.size(); j ++){
                distinctObjCond.add(policyCond.get(j));

            }
        }
        return Lists.newArrayList(distinctObjCond);
    }

    /**
     * Returns the matching policy or null if no match found
     * @param bePolicy
     * @return
     */
    public BEPolicy searchFor(BEPolicy bePolicy){
        return this.getPolicies().stream()
                .filter(bp -> bp.equals(bePolicy)).findFirst().orElse(null);
    }

    /**
     * Given a object condition and an expression, identify the list of policies containing that object condition
     * in the expression
     * @param objectCondition
     * @return
     */
    public void checkAgainstPolices(ObjectCondition objectCondition){
        List<BEPolicy> matchingPolicies = this.policies.stream()
                .filter(pol -> pol.containsObjCond(objectCondition))
                .collect(Collectors.toList());
        this.policies = matchingPolicies;
    }


    /**
     * Given a collection of object conditions and policies, identify the list of policies in
     * the expression containing the collection
     */
    public void checkAgainstPolices(Set<ObjectCondition> objectConditionSet){
        List<BEPolicy> matchingPolicies = this.policies.stream()
                .filter(pol -> pol.containsObjCond(objectConditionSet))
                .collect(Collectors.toList());
        this.policies = matchingPolicies;
    }

    /**
     * Deletes the object condition from the policies
     * @param objectCondition
     * @return
     */
    public void removeFromPolicies(ObjectCondition objectCondition){
        for (int i = 0; i < this.policies.size(); i++) {
            this.policies.get(i).deleteObjCond(objectCondition);
        }
    }

    /**
     * Removes a set of object condition from policies
     * @param objSet
     */
    public void removeFromPolicies(Set<ObjectCondition> objSet) {
        for(int i = 0; i < this.policies.size(); i++){
            //Checking if the policy contains all the object conditions in the multiplier
            if(this.policies.get(i).containsObjCond(objSet)){
                //When objSet is the entire policy, i.e quotient is empty after factorization
                if(this.policies.get(i).getObject_conditions().size() == objSet.size())
                    continue;
                for(ObjectCondition obj: objSet){
                    this.policies.get(i).deleteObjCond(obj);
                }
            }
        }
    }

    /**
     * Replaces the existing object condition (oc1) in Policy Expression with new object condition (oc2)
     * @param oc1
     * @param oc2
     */
    public void replaceFromPolicies(ObjectCondition oc1, ObjectCondition oc2){
        for (int i = 0; i < this.policies.size(); i++) {
            int prev = this.policies.get(i).getObject_conditions().size();
            this.policies.get(i).deleteObjCond(oc1);
            int after = this.policies.get(i).getObject_conditions().size();
            if(prev != after){ //deleted so replace
                this.policies.get(i).getObject_conditions().add(oc2);
            }
        }
    }


    /**
     * Replenishes the existing object condition (oc1) in Policy Expression with new object condition (oc2)
     * Note: This eliminates the need for post filtering to remove false positives
     * @param oc1
     * @param oc2
     */
    public void replenishFromPolicies(ObjectCondition oc1, ObjectCondition oc2){
        for (int i = 0; i < this.policies.size(); i++) {
            if(this.policies.get(i).containsObjCond(oc1)){
                if(!this.policies.get(i).containsObjCond(oc2)) {
                    this.policies.get(i).getObject_conditions().add(oc2);
                }
            }
        }
    }


    /**
     * Given a list of policies, it creates a query by
     * AND'ing object conditions that belong to the same policy
     * and by OR'ing the object conditions across policies
     * @return
     */
    public String createQueryFromPolices(){
        StringBuilder query = new StringBuilder();
        String delim = "";
        for (BEPolicy beP: this.getPolicies()) {
            query.append(delim);
            query.append("(" + beP.createQueryFromObjectConditions() + ")");
            delim = PolicyConstants.DISJUNCTION;
        }
        return query.toString();
    }

    public void parseJSONList(String jsonData) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<BEPolicy> bePolicies = null;
        try {
            bePolicies = objectMapper.readValue(jsonData, new TypeReference<List<BEPolicy>>(){});
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.policies = bePolicies;
    }

    /**
     * Selectivity of a disjunctive expression
     * e.g., A = u or B = v
     * sel = 1 - ((1 - sel(A)) * (1 - sel (B)))
     * @return
     */
    public double computeL(){
        double selectivity = 1;
        for (BEPolicy bePolicy: this.getPolicies()) {
            selectivity *= (1 - bePolicy.computeL(bePolicy.getObject_conditions()));
        }
        return 1 - selectivity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BEExpression that = (BEExpression) o;
        return Objects.equals(policies, that.policies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policies);
    }

    /**
     * Estimates the cost of evaluation of policies as the sum of evaluating individual policies
     * @return
     */
    public double estimateCostForSelection(boolean evalOnly){
        return this.getPolicies().stream().mapToDouble(p -> p.estimateCost(evalOnly)).sum();
    }

    /**
     * TODO: In the case of identical predicates does it include savings from reading only once?
     * Estimates the cost of a evaluating the expression based on the object condition where the object condition
     * is evaluated using index scan and the rest of the policies are evaluated as filter.
     * Selectivity of oc * D * Index access + Selectivity of oc * D * cost of filter * alpha * number of predicates
     * alpha (set to 2/3) is a parameter which determines the number of predicates that are evaluated in the policy
     * @return
     */
    public double estimateCostOfGuardRep(ObjectCondition oc, boolean evalOnly){
        long numOfPreds = this.getPolicies().stream().mapToInt(BEPolicy::countNumberOfPredicates).sum();
        if(!evalOnly){
            return PolicyConstants.NUMBER_OR_TUPLES * oc.computeL() * (PolicyConstants.IO_BLOCK_READ_COST  +
                    PolicyConstants.ROW_EVALUATE_COST * numOfPreds * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED);
        }
        else {
            return PolicyConstants.NUMBER_OR_TUPLES * oc.computeL() *
                    PolicyConstants.ROW_EVALUATE_COST * numOfPreds * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED;
        }

    }

    public BEExpression mergeExpression(BEExpression beExpression){
        BEExpression extended = new BEExpression(this);
        extended.getPolicies().addAll(beExpression.getPolicies());
        return extended;
    }

    /**
     * Removes identical policies with different ids from the expression
     * @return
     */
    public String cleanQueryFromPolices() {
        StringBuilder query = new StringBuilder();
        String delim = "";
        List<BEPolicy> dupElim = new BEExpression(this.getPolicies()).getPolicies();
        for (int i = 0; i < this.getPolicies().size(); i++) {
            for (int j = i + 1; j < this.getPolicies().size(); j++) {
                BEPolicy bp1 = this.getPolicies().get(i);
                BEPolicy bp2 = this.getPolicies().get(j);
                if (bp1.equalsWithoutId(bp2)) {
                    dupElim.remove(bp1);
                    break;
                }
            }
        }
        for (BEPolicy beP : dupElim) {
            query.append(delim);
            query.append("(" + beP.cleanQueryFromObjectConditions() + ")");
            delim = PolicyConstants.DISJUNCTION;
        }
        return query.toString();
    }

}