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

    MySQLQueryManager queryManager = new MySQLQueryManager();

    List<BEPolicy> policies;

    public BEExpression(){
        this.policies = new ArrayList<BEPolicy>();
    }

    public BEExpression(List<BEPolicy> policies){
        this.policies = new ArrayList<BEPolicy>(policies);
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
     * Given a object condition and list of policies, identify the list of policies containing that object condition
     * @param objectCondition
     * @return
     */
    public void checkAgainstPolicies(ObjectCondition objectCondition){
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
     * TODO: What happens when a policy only contains one of the object condition and not the other?
     * @param objSet
     */
    public void removeFromPolicies(Set<ObjectCondition> objSet) {
        for (ObjectCondition obj: objSet) {
            this.removeFromPolicies(obj);
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
     * Given a list of policies, it creates a query by
     * AND'ing object conditions that belong to the same policy
     * and by OR'ing the object conditions across policies
     * @return
     */
    public String createQueryFromPolices(){
        StringBuilder query = new StringBuilder();
        String delim = "";
        for (int i = 0; i < this.policies.size(); i++) {
            query.append(delim);
            query.append("(" + this.policies.get(i).createQueryFromObjectConditions() + ")");
            delim = PolicyConstants.DISJUNCTION;
        }
        return query.toString();
    }

    public long computeCost(){
        return queryManager.runTimedQuery(createQueryFromPolices());
    }

    public double computeFalsePositives() {
        return queryManager.runCountingQuery(createQueryFromPolices());
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
}
